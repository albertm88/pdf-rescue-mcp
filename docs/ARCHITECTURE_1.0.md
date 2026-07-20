# PDF Rescue MCP 1.0 架构

## 边界

MCP 的 Host / Client / Server 是接入协议关系，不替代产品运行分层：

```text
VS Code / TRAE / Codex / AnythingLLM / 其他 Host
                    │ stdio 或仅本机回环 HTTP
                    ▼
              FastMCP adapter
                    │ 不执行 OCR
 ┌──────────────────┼──────────────────┐
 ▼                  ▼                  ▼
业务层            监管层              迭代更新层
OCR/缓存/产物     任务账本/租约/治理  质量证据到建议计划
```

任一 MCP 适配器默认是短生命周期入口；当存在未完成批次时，监管层用本机 fencing lease 选出唯一的 **controller**。其余 VS Code、TRAE、Codex、AnythingLLM 或 HTTP 适配器都是 **observer**：只能读取同一份监管快照，不能因为一次查询而恢复 watcher、扫描书库、启动 worker 或改写账本。observer 按账本 mtime 只读刷新；只有原 controller 的 lease 失效后，后台 failover 线程才会竞争接管，接管成功后先恢复任务监管、再恢复批量调度。实际 OCR 任务属于本机监管层，而不是某个对话、某个 IDE 或一次 MCP 请求。

## 三层职责

| 层 | 核心组件 | 允许做什么 | 明确不做什么 |
|---|---|---|---|
| 业务层 | `book_pipeline`、OCR worker、逐页缓存 | 解析 PDF、OCR、写原子状态/页缓存/质量证据 | 不重启自己、不判断进程树、不自行改策略 |
| 监管层 | `LocalSupervisor`、`TaskStore`、`ProcessController`、`_TaskManager` | 幂等任务、SQLite lease、尝试记录、心跳、页级前进、取消、恢复、进程树收尾 | 不在 MCP 主进程跑 OCR、不保存密码、不修改识别规则 |
| 迭代更新层 | `iteration.build_iteration_plan` | 从状态、质检、监管事件形成版本化建议和回滚要求 | 不自修改代码、不自动重跑、不联网、不直接写入业务产物 |

## 非阻塞 OCR 生命周期

1. MCP 工具只做有限的文本层规划；交互规划不执行 `nvidia-smi` 或外部工具版本探测。完整运行时体检由监管层/显式健康检查完成。
2. 如果需要 OCR，则始终启动独立 worker，即使调用方传入 `foreground`。
3. 已知没有 OCR 引擎时返回稳定的 `blocked / ocr_runtime_unavailable`，绝不创建必败 worker。
4. 监管层为“源文件指纹 + 输出根目录 + 模式 + 页范围”创建/复用本机任务，并取得 fencing lease。
5. worker 获得不含密码的任务数据库路径和尝试 ID；密码只通过临时子进程环境传递。
6. worker 在每页开始时记录“当前页”，在缓存、状态、低置信/失败记录完成原子提交后才记录“最后完成页”。
7. 监管层每 5 秒检查两类信号：进程心跳与页级前进。心跳仍在但当前页超时同样被视为卡页。
8. 卡死先写协作停止请求，等待页边界；超时后由 `psutil` 递归 `terminate → wait → kill`，再基于页缓存恢复一次。
9. 终态或失败会结算尝试、释放精确 lease；恢复产生新的尝试号而不覆盖历史。

## 批量书本计数与动态 worker

`get_batch_status` 同时返回 `书本完成数`、`书本总数`、`书本失败数`、`书本待处理数` 和 `进行中书本数`，与当前书籍的页级进度分开统计，避免把“页完成”误报成“书完成”。只有实际处理页数覆盖源 PDF 页数时，书本才计入完成数。

`batch_extract_library` / 目录形式的 `rescue_pdf` 先进入 `准备中`，由监管层后台发现书库后才转入 `运行中`；扫描不能占用 MCP 请求线程。批量账本拥有单独的 controller lease，controller 每轮续租；失去 lease 的进程立即停止 admission/恢复并降为 observer。observer 的 `stop_batch` 只写入停止命令，由 controller 在下一个监管周期执行，因此不会形成第二个写者。

资源、RSS、逐线程 CPU、页速和 worker 调度计划都在 controller 的监管周期中采样并持久化为快照。状态工具（包括 observer）只读最近快照并带采样时间；频繁查询不会重新采样 worker、重新计算调度、修改页速基线或与调度锁竞争。

批量调度由 `resource_scheduler.ResourceScheduler` 给出容量决策：

- 以逻辑 CPU 线程预算、每个 worker 的活跃/饱和线程与逐线程 0–100% 采样作为主约束；进程 CPU 统一归一化为整机 0–100%，多线程累计负载单列为等效核心数，整机 CPU 主要用于识别外部负载护栏；
- 以系统可用内存、保留水位和每个 worker 的 RSS 预算作为内存约束；
- 只有资源有余量时才增加 worker；高 CPU 或低内存时保持当前并发，不强杀正在运行的 worker；
- 每个 worker 同时获得线程预算（`PDF_RESCUE_OCR_THREADS`），避免多进程各自抢占全部 CPU；
- 可用 `max_workers` 或 `PDF_RESCUE_MAX_WORKERS` 设置硬上限，默认仍由运行时动态规划。

## 吞吐容量调优（迭代更新层）

容量调优不是在运行中修改 Paddle/OCR adapter 的线程数。业务层仅在真正调用 OCR 时记录滚动页耗时、短窗页/分钟与启动时固定的线程预算；缓存命中、原生文本页和预热页不污染吞吐样本。

监管层同时采样每个 worker 的线程 CPU（每条线程均为 0–100%）、活跃/饱和线程数、等效核心数、RSS、可用内存与外部 CPU 负载。调度下一本书时优先看已占用线程槽和下一 worker 的线程预算；整机 CPU 只作为外部负载护栏。运行中的 worker 绝不被热改或为扩容而重启。

迭代更新层的 `plan_ocr_capacity_profile` / `start_ocr_capacity_profile` / `get_ocr_capacity_profile` / `activate_ocr_capacity_profile` 构成独立闭环：在无生产 OCR 时，以私有、不重叠的页 fixture 串行测试候选，候选内部允许多 worker 并行；记录真实页吞吐、质量、RSS 和线程利用率。16 逻辑线程默认包含单 worker `1x2/1x4/1x6/1x8`，以及保留系统线程后不超预算的多 worker 组合。失败页、低置信质量回退、内存保留水位或 CPU 护栏失败的结果不能被推荐。结果始终为 advisory-only，且只有全部候选结算、状态为“完成”后，用户显式激活才会影响后续新 worker。

容量基准在发现生产 OCR 时返回“已延期”；启动握手期间等待 worker 首个心跳，取消、超时或中途发现生产 OCR 时会向自身 worker 发安全停止请求。基准的私有运行目录按 profile 与 run ID 分隔，重跑不会覆盖已有证据。

## 持久化与恢复

`TaskStore` 使用本机 SQLite WAL，记录任务、尝试、页状态、追加事件和带 token 的租约。它不是跨机器数据库：不得置于网络共享盘、云盘同步目录或多写者共享文件系统。

状态文件和 JSONL 均使用同目录临时文件加 `os.replace` 提交，轮询者不会把半份 JSON 当作 OCR 失败。旧 worker 或监管进程崩溃后，只有在确认 worker 已退出且 lease 失效时，任务才可作为孤儿重新排队。

## 跨平台策略

| 能力 | Windows | Linux / macOS | 共同正确性基线 |
|---|---|---|---|
| worker 启动 | `CREATE_NEW_PROCESS_GROUP` + 无窗口标志 | `start_new_session=True` | `subprocess.Popen(shell=False)` |
| 任务结束 | 可选组语义 | 可选 session 语义 | 用 PID + create time 验证后递归 psutil 收尾 |
| 状态目录 | `%APPDATA%` / `%LOCALAPPDATA%` | macOS Library / Linux XDG | `PDF_RESCUE_RUNTIME_ROOT` 可指定绝对便携目录 |
| OCR 后端 | 取决于安装的 Paddle/CPU/GPU 包 | 取决于安装的 Paddle/CPU/GPU 包 | MCP、监管、缓存、恢复不依赖某个平台专有 API |

## 对客户端的契约

- 默认传输为 stdio；支持 MCP 的 Host 可直接使用公开的 snake_case tool 名。
- 可选 Streamable HTTP 只绑定回环地址；跨机器访问必须由带认证的网关负责。
- 普通 Host 首选只调用 `rescue_pdf(path=<PDF或目录>, request=<用户原话>)`。返回中的机器字段 `contract_version` 和 `next_call` 使用固定英文键（`tool`、`arguments`、`read_only`），让任何 LLM/Host 无需解析中文展示文本或猜测后续工具。
- `get_job_status` 与 `get_batch_status` 是显式只读 MCP 工具；它们返回业务、监管和资源快照，但不会启动/恢复 OCR。
- 其余生命周期、容量和迭代工具保持兼容入口，供明确的管理员流程调用；不依赖实验性 MCP Tasks 能力。
- `tools/list` 是发布契约。新增工具必须同步更新测试和客户端文档。

## 更新治理

`get_iteration_plan` 返回的每个动作均为 `advisory_only`，并标注证据、审批要求和回滚语义。批准后的重跑必须新建可比较的输出版本，记录规则版本与责任人；不可让运行中的 worker 静默切换规则或由模型自行修改代码。

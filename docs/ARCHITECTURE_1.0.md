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

任一 MCP 适配器都是短生命周期的无状态入口；实际 OCR 任务属于本机监管层，而不是某个对话、某个 IDE 或一次 MCP 请求。

## 三层职责

| 层 | 核心组件 | 允许做什么 | 明确不做什么 |
|---|---|---|---|
| 业务层 | `book_pipeline`、OCR worker、逐页缓存 | 解析 PDF、OCR、写原子状态/页缓存/质量证据 | 不重启自己、不判断进程树、不自行改策略 |
| 监管层 | `LocalSupervisor`、`TaskStore`、`ProcessController`、`_TaskManager` | 幂等任务、SQLite lease、尝试记录、心跳、页级前进、取消、恢复、进程树收尾 | 不在 MCP 主进程跑 OCR、不保存密码、不修改识别规则 |
| 迭代更新层 | `iteration.build_iteration_plan` | 从状态、质检、监管事件形成版本化建议和回滚要求 | 不自修改代码、不自动重跑、不联网、不直接写入业务产物 |

## 非阻塞 OCR 生命周期

1. MCP 工具规划路径；如果需要 OCR，则始终启动独立 worker，即使调用方传入 `foreground`。
2. 监管层为“源文件指纹 + 输出根目录 + 模式 + 页范围”创建/复用本机任务，并取得 fencing lease。
3. worker 获得不含密码的任务数据库路径和尝试 ID；密码只通过临时子进程环境传递。
4. worker 在每页开始时记录“当前页”，在缓存、状态、低置信/失败记录完成原子提交后才记录“最后完成页”。
5. 监管层每 5 秒检查两类信号：进程心跳与页级前进。心跳仍在但当前页超时同样被视为卡页。
6. 卡死先写协作停止请求，等待页边界；超时后由 `psutil` 递归 `terminate → wait → kill`，再基于页缓存恢复一次。
7. 终态或失败会结算尝试、释放精确 lease；恢复产生新的尝试号而不覆盖历史。

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
- 所有长任务返回 `job_dir`，随后通过 `get_job_status`、`resume_job`、`cancel_job`、`audit_job_quality` 和 `get_iteration_plan` 操作；不依赖实验性 MCP Tasks 能力。
- `tools/list` 是发布契约。新增工具必须同步更新测试和客户端文档。

## 更新治理

`get_iteration_plan` 返回的每个动作均为 `advisory_only`，并标注证据、审批要求和回滚语义。批准后的重跑必须新建可比较的输出版本，记录规则版本与责任人；不可让运行中的 worker 静默切换规则或由模型自行修改代码。

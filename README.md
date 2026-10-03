# 地面站高危遥控值班系统（Select-before-Execute）

地面站向载荷下发**高危遥控**前，值班系统强制采用"先选择、后执行"的两段式裁决。
服务仅依赖 Python 3.11 标准库（无需安装第三方包）。

## 裁决规则

| 场景 | 裁决 |
|---|---|
| 创建设备选择（设备标识 + 稳定操作标识 + 命令摘要 + 失效时刻） | `CHOICE_CREATED` (201) |
| 相同内容的选择/执行重传 | 回放原裁决：`CHOICE_REPLAYED` / `EXEC_REPLAYED`，返回同一最终结果 |
| 同一操作标识但摘要/失效时刻变化 | `CHOICE_CONFLICT` (409)，明确冲突 |
| 同设备已有另一未失效选择，再选别的操作标识 | `CHOICE_DEVICE_BUSY` (409) |
| 成功执行后再次选择（任何操作标识） | `CHOICE_DEVICE_EXECUTED` (409) |
| 执行请求操作标识与有效选择不符 | `EXEC_WRONG_OP` (409) |
| 执行请求摘要与选择不一致 | `EXEC_SUMMARY_MISMATCH` (409) |
| 选择已过期仍要求执行 | `EXEC_EXPIRED` (410)，绝不下发 |
| 两个并发执行请求竞争同一有效选择 | 恰一个 `EXEC_ACCEPTED`，其余阻塞后收到**同一最终结果** `EXEC_REPLAYED`，物理下行仅一次 |
| 持久记录无法通过校验 | 健康检查 503，设备被隔离（`EXEC_QUARANTINED`） |

## 断电恢复

每条记录均带 SHA-256 校验和，先追加 `fsync` 记录日志，再以 `tempfile + os.replace`
原子发布设备索引：

- **结果落盘前**中断（已写 executing 标记、下游幂等已送达）：重启恢复为
  `SELECTED`（可安全重试）；重试经下游幂等去重，物理命令仍只下发一次。
- **结果落盘后**中断：重启直接恢复为 `EXECUTED`，重放完全相同的结果回执。
- 旧选择的记录文件不再被索引引用，**不会复活**。
- 撕裂写入/校验和不符的记录在恢复时被检出，`/healthz` 返回 `unhealthy`。

## HTTP API

```
POST /v1/choices       {"device_id","op_id","summary","expires_at"}
POST /v1/executions    {"device_id","op_id","summary"}
GET  /v1/devices/<id>
GET  /healthz
```

执行接口的 `crash: "before_result" | "after_result"` 为验收专用的断电注入开关
（以退出码 99 硬终止进程）。

## 运行

```bash
# 本地直接运行
DATA_DIR=./data PORT=8080 python3 -m app.server

# 单元测试
python3 -m unittest discover -s tests -t .
```

## Compose 一次性验收服务

```bash
docker compose up --build verify
```

`verify` 为**可执行一次性**服务（`rest: "no"`），顺序完成并以退出码报告：

1. 代码测试（unittest：裁决、并发竞争、断电恢复、校验和完整性）
2. 构建检查（compileall）
3. 容器内 HTTP/API 冒烟，实际覆盖：
   - 健康检查与选择—执行全流程（重传回放、字段冲突、他操作标识拒绝）
   - **并发**：6 个并发执行请求竞争同一有效选择，仅一个首次消费成功
   - **过期**：选择过期后执行被拒，新操作标识可替换且旧选择失效
   - **断电恢复 ×2**：结果落盘前/后分别注入硬中断，重启新进程后验证
     安全重试 / 结果重放，且物理下行跨重启仅一次
   - 持久记录损坏 → `/healthz` 503
4. 经 compose 网络对常驻 `duty` 服务做健康与选择—执行冒烟（`BASE_URL`）

成功时末行打印 `ALL ACCEPTANCE CHECKS PASSED` 并以 `0` 退出；
任何检查失败则以非零退出码报告。

## 目录结构

```
app/store.py     校验和、原子写、崩溃恢复的持久化层
app/gateway.py   模拟载荷设备网关（按操作标识幂等，记录物理下发次数）
app/service.py   选择/执行裁决（每设备锁串行化，竞争方只可见终态）
app/server.py    stdlib ThreadingHTTPServer 前端
tests/           单元测试
scripts/acceptance.py  一次性验收脚本（compose verify 的入口）
```

# 实现计划：`--throughput` 带宽限流（字节/秒）

## 目标
新增一个与 qps 正交的**带宽限流**维度：`--throughput 1MB/s` 让压测按「写出的 value
字节数/秒」节流。第一版仅覆盖 string 纯写场景，复用现有「每 worker 绝对时刻表」骨架。

## 已达成的设计共识（决策已锁定）

| # | 决策 | 结论 |
|---|------|------|
| 1 | 限流量纲 | 带宽 = **写出的 value 字节数**，与 qps 正交 |
| 2 | 字节口径 | 只算写路径 value payload，不含 key/协议/读回包 |
| 3 | 与 `--ops` 关系 | **互斥**，同时给报错 |
| 4 | 全局 vs 每 worker | 全局总带宽，按 client 均分（与 `--ops` 同款） |
| 5 | 进度累加 | 累加**真实字节** `bytesDone`，drift-free；size 由 `fillWrite` 回传 |
| 6 | deadline 算法 | `float64(bytesDone) * nsPerByte` 现算（防整数塌零 + 防 int64 溢出） |
| 7 | 过高带宽 | `nsPerByte` 低于阈值时 stderr 温和提示，**不报错、不退化** |
| 8 | flag | `--throughput`，值 `1MB/s`（`B/KB/MB/GB`，`/s` 可选，**1KB=1024**） |
| 9 | 适用范围 | 仅 string 纯写；与命令模式 / 非 string `-t` / 含 GET 的 `--ratio` 同现均报错 |
| 10 | 可观测性 | 始终累加 `bytesWritten`，report + summary + JSON(`bytes_total`/`mbps`) 显示实际带宽，同口径 |
| 11 | `--load` 阶段 | **不受带宽限流**（预热尽快填满，与 `--ops` 对 load 的处理一致） |
| 12 | 测试 | 纯函数层为主力；e2e 用 `s.bytesWritten` 宽容断言；单测锁字节累加口径；增量覆盖率 ≥ 80% |
| 13 | 文档 | 需说明**变长 value（`--data-size-range`）下带宽的短期抖动**（长期收敛，短期按 rand 采样波动） |

## 改动清单

### 1. `main.go` — flag + 解析 + 校验
- 新增包级变量 `throughput string`（原始 flag 值）。
- `Flags().StringVar(&throughput, "throughput", "", "限制写出的 value 字节速率，如 1MB/s / 500KB（B/KB/MB/GB，1KB=1024，/s 可选）；与 --ops 互斥，仅 string 纯写")`。
- 新增 `parseThroughput(s string) (bytesPerSec int64, err error)`：
  - 宽松大小写；识别可选 `/s` 后缀后剥离；识别 `B/KB/MB/GB`（1024 进制）；无单位后缀视为 `B`。
  - 非法输入、负数、零 → 报错。
- `validateFlags()` 增加互斥/范围校验（只有当 `throughput != ""` 时触发）：
  - 与 `--ops`（`Flags().Changed("ops")` 或 `ops != 默认`）互斥 → 报错。**注意**：`ops` 默认值非 0，需用 `cmd.Flags().Changed("ops")` 判断用户是否显式设过，避免误伤默认值；此处 `validateFlags` 拿不到 cmd，需把互斥判断挪到 `PreRunE`（已能拿到 cmd）或给 validateFlags 传入 changed 标志。
  - `len(commands) > 0`（命令模式）→ 报错。
  - `dataTypes` 含非 string → 报错。
  - `--ratio` 的 GET 权重 > 0 → 报错（强制纯写）。
  - `parseThroughput` 解析失败 → 报错。
- `run()`：解析出 `bytesPerSec`，传入 `NewSender`（见下）。

### 2. `sender.go` — 限流骨架推广 + 字节计数
- `Sender` 结构体新增：
  - `nsPerByte float64`（0 表示不按带宽限流）。
  - `bytesWritten atomic.Int64`（始终累加，供 report/JSON）。
- `NewSender` 签名扩展：接收 `bytesPerSec int64`。换算 `nsPerByte`：
  ```
  if bytesPerSec > 0 && clientNum > 0 {
      perWorkerBps := float64(bytesPerSec) / float64(clientNum)
      nsPerByte = 1e9 / perWorkerBps
  }
  ```
  - Q7 温和提示：`if nsPerByte > 0 && nsPerByte < throughputPrecisionFloorNs { fmt.Fprintf(os.Stderr, "warning: per-worker throughput very high (~%.0f ns/byte), rate limiting may be imprecise\n", nsPerByte) }`。常量 `throughputPrecisionFloorNs = 1.0`（≈ >1GB/s/worker）。
- `worker`（string 路径）：
  - jitter 起步条件从 `perOpTickNs > 0` 改为 `perOpTickNs > 0 || nsPerByte > 0`；jitter 相位在带宽模式下用「一个平均批的应耗时」估算（见下 startup jitter）。
  - 循环内维护 `var bytesDone int64`（写字节累加，仅本 worker）。
  - `fillWrite` 回传本次 value size（见 workload 改动），worker 累加：`bytesDone += size`，同时 `s.bytesWritten.Add(int64(size))`（每条累加或每批汇总累加——见下「字节累加粒度」）。
  - 节流分支：qps 与带宽**互斥**，用 if/else 二选一：
    ```
    if s.perOpTickNs > 0 {
        deadline := startTime.Add(time.Duration(opsDone * s.perOpTickNs))
        ...throttleWait...
    } else if s.nsPerByte > 0 {
        deadline := startTime.Add(time.Duration(float64(bytesDone) * s.nsPerByte))
        if d := time.Until(deadline); d > 0 { if !throttleWait(...) { return } }
    }
    ```
- `workerCommand`：带宽模式不会走到这里（命令模式已被校验挡掉），无需改限流；但 `bytesWritten` 在命令模式是否累加？→ 命令模式下无带宽限流，但为「始终显示实际带宽」一致性，命令模式的 `__data__` 字节也累加进 `bytesWritten`（Q10 决定「始终显示」）。实现：`workerCommand` 里 `val := ...; s.bytesWritten.Add(int64(len(val)))`。

**字节累加粒度**（实现细节，取向已定为「真实字节」）：
- string 写批：循环内 `fillWrite` 逐条回传 size，worker 局部累加成 `batchBytes`，批末一次 `bytesDone += batchBytes` + `s.bytesWritten.Add(batchBytes)`（每批一次原子加，不破低分配）。
- 定长 value：`batchBytes = pipeline * dataSize`，可循环外直接算，省去逐条求和。

**startup jitter（带宽模式）**：现有 jitter = `rand(perOpTickNs*pipeline)`。带宽模式改用平均批耗时：`avgBatchNs = float64(pipeline) * avgValueSize * nsPerByte`，jitter = `rand(avgBatchNs)`。`avgValueSize` 定长时 = dataSize，变长时 = (min+max)/2。

### 3. `workload.go` — `fillWrite` 回传 size
- `fillWrite` / `fillWriteAt` 返回本次写入的 value 字节数（string 为 `pickValueSize` 结果；非 string 返回 0，因为第一版不限非 string，且 `bytesWritten` 对非 string 无意义）。
  - 签名：`func (w *Workload) fillWrite(st, op) int`（返回 size）。
  - 现有调用点（`worker`、`Load`）相应接收返回值；`Load` 可忽略返回值（预热不限流、不计带宽）。

### 4. `output.go` — JSON 增字段
- `runReport` 增 `BytesTotal int64 \`json:"bytes_total"\`` 和 `MBPS float64 \`json:"mbps"\``。
- `buildReport`：`rep.BytesTotal = s.bytesWritten.Load()`；`rep.MBPS = float64(bytesTotal)/elapsed.Seconds()/(1024*1024)`（1024 进制，与 flag 口径一致）。

### 5. `sender.go report()` — 实时 + summary 带宽列
- report 的采样循环额外记 `lastBytes`，算瞬时 `MB/s = (curBytes-lastBytes)/elapsed/(1024*1024)`。
- 追加一列 `mb/s: %.2f`（qps 模式也显示，Q10 决定「始终」）。
- `printSummary` 追加一行或一列显示 `total: X MB, avg: Y MB/s`。

### 6. `README.md` / `CLAUDE.md` — 文档
- README 参数表加 `--throughput` 行；新增一节说明：
  - 口径：带宽 = 写出的 value 字节数，不含协议/key/读回包。
  - 约束：与 `--ops` 互斥；仅 string 纯写（含 GET 的 ratio / 非 string / 命令模式报错）。
  - `--load` 阶段不受限。
  - **变长 value 抖动**（Q13）：`--data-size-range` 下每 worker 按各自 rand 采样 value 大小，全局带宽**长期收敛到目标值，短期有抖动**（单批字节数不定 → 瞬时 MB/s 波动），定长 value 无此抖动。
- CLAUDE.md 架构要点补一句：「限流有两个互斥维度——qps（`perOpTickNs`，按条）与带宽（`nsPerByte`，按写出 value 字节）；均为每 worker 绝对时刻表、drift-free；带宽仅 string 纯写。」

## 测试计划（增量覆盖率 ≥ 80%）

### 纯函数（无需 redis，覆盖率主力）
- `parseThroughput`：`1MB/s`/`500KB`/`1GB/s`/`1024B`/`2M`（无 /s）→ 正确字节值；`1KB==1024`；非法（空单位歧义、负数、`0`、乱码、`1XB`）→ 报错。
- 校验组合（`validateFlags`/PreRunE 层）：`--throughput` + `--ops`(显式) / + `--command` / + `-t list` / + `--ratio 1:9` 均报错；`--throughput 1MB/s -t string --ratio 1:0` 通过。
- `nsPerByte` 换算 + deadline 单调性：给定递增 `bytesDone` 序列，`float64(bytesDone)*nsPerByte` 单调不减、大值不溢出（跑到 1e12 字节量级验证 float 不塌）。
- **口径锁定测试**：构造 Workload，调 `fillWrite` 断言返回值 == `len(op.strVal)` == pickValueSize 逻辑，锁死「累加字节 == 实际 value 字节」（补 e2e 自证漏洞）。

### e2e（`REDIS_ADDR`，连不上 skip）
- `TestSenderThroughputLimit`（孪生自 `TestSenderRateLimit`）：
  - `--throughput 1MB/s`、定长 value、`-d 2s`，跑完断言 `s.bytesWritten.Load()` ∈ `[~1MB, ~1MB*(2+tolerance)]`（上界宽容，和 ops 用例同款 `sec+N` 思路）。
  - 用唯一 `key-prefix`，只 `SCAN`+`DEL` 自己的 key，不 FLUSHDB。

## 验证步骤
```bash
go vet ./... && gofmt -l *.go
go build -o redis-benchmark-go .
go test ./...                                   # 纯函数 + e2e(无 redis 自动 skip)
redis-server --port 6399 --save '' --appendonly no --daemonize yes --dir /tmp
REDIS_ADDR=127.0.0.1:6399 go test -race ./...   # 含带宽 e2e
redis-cli -p 6399 shutdown nosave
# 手工冒烟：观察实际带宽是否收敛到 1MB/s
./redis-benchmark-go -a 127.0.0.1:6399 --throughput 1MB/s -d 5s --data-size 1024 -t string
```

## 不做（第一版明确排除，可后续扩展）
- 命令模式 / 非 string 类型的带宽限流。
- 读回包带宽、含协议帧的精确字节口径。
- `--ops` 与 `--throughput` 叠加（max-of-two）。
- 修既有 qps 路径 `opsDone*perOpTickNs` 的潜在溢出（本次只在新带宽路径用 float 算法，不动既有代码）。

## 风险 / 待实现时定夺的小点
- **`--ops` 互斥判断**依赖 `cmd.Flags().Changed("ops")`（默认值非 0），须在 `PreRunE` 层做（能拿到 cmd），或把 changed 标志传进 `validateFlags`。倾向前者。
- Q7 精度提示阈值常量 `throughputPrecisionFloorNs`，实现时定为 1.0 ns/byte 并写进提示文案，非硬性。

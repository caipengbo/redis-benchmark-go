# CLAUDE.md

面向在本仓库工作的 AI agent 的项目指引。**Redis 压测工具（Go）**：多 worker + pipeline、可配置的 key 分布与 value 大小、读写混合、任意命令模式、限流/全速、延迟分位统计。

## 构建 / 运行 / 测试

```bash
go build -o redis-benchmark-go .        # 构建
go vet ./... && gofmt -l *.go           # 提交前检查（gofmt -l 应无输出）
go test ./...                           # 单元测试（无 redis 时 e2e 自动 Skip）
REDIS_ADDR=127.0.0.1:6379 go test -race ./...   # 含 e2e，需可用 redis
```

改完代码务必先 `go vet` + `go build` + `go test`；能连 redis 时用 `-race` 跑 e2e。

### e2e 需要一个可用 redis
- e2e 用例通过环境变量 `REDIS_ADDR`（默认 `127.0.0.1:6379`）连接；**连不上则自动 `t.Skip`**，不会让 CI 失败。
- 本地临时起一个即可，例如：
  ```bash
  redis-server --port 6399 --save '' --appendonly no --daemonize yes --dir /tmp
  REDIS_ADDR=127.0.0.1:6399 go test ./...
  redis-cli -p 6399 shutdown nosave
  ```

## 文件结构

- `main.go` — cobra CLI：包级 flag 变量、`main()` 里 `Flags().XxxVar` 绑定、`PreRunE`→`validateFlags`、`run()` 编排。`-P` 预设文件解析（`applyWorkloadFile`）。
- `generator.go` — 分布生成器：`Generator` 接口 + `Counter/Sequential/Uniform/Zipfian/ScrambledZipfian` + `hash64`。`number.lastValue` 用原子（生成器实例被多 worker 共享）。
- `workload.go` — `Workload`（共享配置 + key chooser）+ `workerState`（每 worker 私有 rand/valBuf/keyB）。`buildKeyNameStd`、`fillWrite/fillKey/fillWriteAt`、`NewWorkload(WorkloadConfig)`。
- `command.go` — 任意命令模式：`commandSpec`（模板 tokens + 独立 keyChooser/ratio/hist/count）、`valueSizer`、`buildArgs`（替换 `__key__`/`__data__`）。
- `data_generator.go` — `Type` 常量、`IsSupportedType`、`typedValue`（非 string 的定值 payload）。
- `sender.go` — 多 worker 引擎：`worker`（类型 workload 路径）/`workerCommand`（命令模式路径）、`Load`（预热）、`report`/`printSummary`、`addToPipeline`。
- `histogram.go` — 封装 HdrHistogram（`sync.Mutex` 保护，按批记录），`record`/`percentile`/`stats`/`exportSnapshot`。
- `output.go` — JSON 摘要（`--json-out`，含 QPS/分位，人读）；标准 HdrHistogram `.hlog` 导出（`--hist-out`，`HistogramLogWriter` + 每 op 一条带 Tag 的 V2 直方图，供任意 HdrHistogram 工具/库合并绘图）。

## 架构要点（改动时必须遵守）

- **无中心分发、无共享 limiter**：每个 worker 自产自销。限流是**每 worker 绝对时刻表**：`perOpTickNs = clientNum*1e9/ops`，节流到 `startTime + opsDone*perOpTickNs`（drift-free、无锁、落后不补偿爆发）。不要退回令牌桶/共享 limiter。
- **两个互斥的限流维度**：qps（`perOpTickNs`，按命令数）与带宽（`nsPerByte`，按写出的 value 字节，deadline 用 `startTime + float64(bytesDone)*nsPerByte` 现算，防整数塌零/溢出）。均为每 worker 绝对时刻表、drift-free。带宽仅 string 纯写（`--throughput`，校验挡掉命令模式/非 string/含 GET），`--load` 不受限；`--throughput` 会把 `ops` 强制归零（`effectiveOps`）以免默认 qps 抢限流。
- **`--ops 0` = 全速**（不建 limiter）；>0 = 全局总速率按 client 均分。
- **启动错峰**：每 worker 起步前随机 sleep 一个相位。
- **低分配**：worker 复用 `Operation`/value 缓冲；`typedValue` 只算一次只读共享；热路径只留 key 一次 string 分配。新增热路径代码要保持低分配。
- **延迟统计**：读写各一个 HdrHistogram，命令模式每命令一个。按 pipeline 批记录 Exec 往返延迟（非单条）。
- **连接**：每 client `PoolSize:1`，总连接数 == `-c`。多地址 `-a` 轮询均分到各后端。

## 关键语义 / 易错点

- **key 格式 `{key-prefix}{number}`**，number 来自分布 ∈ `[key-minimum, key-maximum]`。**多个 `-t` 类型共用同一 key 空间会撞 key（WRONGTYPE）**——需不同 `--key-prefix` 或单类型。
- **读命中**：读要命中需先 `--load`（顺序写满 key 空间）或 `--ratio` 含写预热；否则大量 miss。
- **`--ratio SET:GET` 按“批”选读写**（非单条），便于读写延迟分离，长期收敛到比例。**内置读只支持 string 的 GET**，非 string 恒纯写（`NewWorkload` 强制 `getW=0`）。
- **任意命令 / 非 string 读**用命令模式 `--command`，`__key__`/`__data__` 占位；指定后**绕过 `--ratio`/`-t`/type 系统**，`--load` 被忽略。
- **`-P` 预设文件是本项目内部参数（key=value，`#` 注释）**；未知键报错，CLI 覆盖文件（靠 `Flags().Changed`/`Set`）。
- **flag 多值语义**：`--address`/`--types` 用 `StringSlice`（按逗号拆）；`--command`/`--command-key-pattern` 用 `StringArray`（**不拆**，命令含空格）；`--command-ratio` 用 `IntSlice`。命令按下标与 ratio/pattern 配对。
- **TTL**：`--expiry-range`(秒,随机) 优先于 `--expire`(固定)；String 用 `SET EX`，其它类型额外 `EXPIRE`（命令数翻倍）。

## 测试约定

- e2e 连不上 redis 用 `t.Skip`（`REDIS_ADDR` 或默认 `127.0.0.1:6379`）。
- 每个 e2e 用例用带时间戳的唯一 `key-prefix`，只 `SCAN`+`DEL` 自己的 key，**禁止 FLUSHDB**（勿污染他人数据）。
- 生成器/类型相关测试无需 redis。

## 依赖

`go-redis/v9`、`spf13/cobra`、`HdrHistogram/hdrhistogram-go`。Go 1.21。

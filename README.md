# redis-benchmark-go

一个比原生 `redis-benchmark` 更灵活的 Redis 压测工具，用 Go 编写。支持**读写混合负载、可配置的 key 分布、value 大小分布、load 预热阶段、多地址（proxy 集群）分发、限流/全速两种模式、HdrHistogram 延迟分位**。

> 本文档面向后续的开发者与 AI agent，除使用说明外，还沉淀了架构设计与压测调优经验。

## 目录
- [快速开始](#快速开始)
- [命令行参数](#命令行参数)
- [Workload 机制](#workload-机制)
- [使用示例](#使用示例)
- [预设文件 -P](#预设文件--p)
- [针对 Proxy 集群的压测](#针对-proxy-集群的压测)
- [架构设计](#架构设计)
- [性能与调优经验](#性能与调优经验)
- [测试](#测试)

## 快速开始

```bash
# 编译
go build -o redis-benchmark-go .

# 纯写：5 万 QPS 向本地 Redis 写 string，持续 1 分钟
./redis-benchmark-go -a 127.0.0.1:6379 --ops 50000 -d 1m -t string

# 读写混合：先 load 预热 key 空间，再按 SET:GET=1:9 压测
./redis-benchmark-go -a 127.0.0.1:6379 --load --ratio 1:9 --key-pattern Z --ops 100000
```

## 命令行参数

基础：

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--address` | `-a` | localhost:6379 | Redis/Proxy 地址；可重复或逗号分隔，把连接分散到多个节点 |
| `--password` | `-p` | 空 | 访问密码 |
| `--client` | `-c` | 50 | 客户端数；每个占 1 条连接 |
| `--duration` | `-d` | 24h | 运行时长，单位 s/m/h，须 ≥ 1s（不含 load 阶段） |
| `--types` | `-t` | string | 数据类型，逗号分隔：string,list,set,hash,zset |
| `--pipeline` | | 16 | 每批 pipeline 的命令数 |
| `--fields` | | 8 | hash/zset/set/list 的字段数 |
| `--ops` | | 10000 | 全局发送速率(命令/秒)，**0 表示全速无限流** |
| `--version` | `-v` | | 打印版本 |

Workload 相关：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--ratio` | 1:0 | SET:GET 比，如 1:10。**GET 仅 string 类型**，非 string 恒纯写 |
| `--key-pattern` | R | key 分布：R=均匀 S=顺序 Z=zipfian |
| `--key-minimum` | 0 | key id 下界 |
| `--key-maximum` | 10000000 | key id 上界（key 空间大小） |
| `--key-zipf-exp` | 0.99 | zipfian 指数 (0,5)；=0.99 走 O(1) scrambled 快路径，其它值在真实区间上构建（O(区间)） |
| `--zero-padding` | 0 | 把数字 key id 左补零到该宽度 |
| `--key-prefix` | rbg- | key 前缀；key 格式 `{prefix}{number}` |
| `--data-size` | 32 | string value 字节数 |
| `--data-size-range` | 空 | 随机 value 大小 min-max（覆盖 --data-size） |
| `--random-data` | false | value 填随机字节（默认填固定字节 'x'） |
| `--expire` | 空 | 固定 TTL，如 30s/5m/1h |
| `--expiry-range` | 空 | 随机 TTL（单位秒）min-max（覆盖 --expire） |
| `--load` | false | 先顺序写满整个 key 空间再进入压测（让读能命中） |
| `--workload` | `-P` 空 | 从预设文件加载内部参数（CLI 覆盖之） |

任意命令模式（见下节）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--command` | 空 | 任意命令模板（可重复），含 `__key__`/`__data__` 占位符；一旦指定即进入命令模式，忽略 `--ratio`/`-t` |
| `--command-ratio` | 1 | 第 i 个 `--command` 的权重（按顺序配对） |
| `--command-key-pattern` | =`--key-pattern` | 第 i 个 `--command` 的 key 分布（R/S/Z） |

## Workload 机制

每个操作按如下顺序确定：

1. **读还是写** —— 按 `--ratio` SET:GET 的权重决定。为了让每批 pipeline 的往返延迟能干净地归入读或写，**读写是按“批”选择的**（不是按单条），长期看仍收敛到设定比例。
2. **命中哪个 key** —— 在有界 key 空间 `[--key-minimum, --key-maximum]` 内按 `--key-pattern` 选一个编号：`R` 均匀、`S` 顺序、`Z` zipfian（热点集中）。key 名 = `{--key-prefix}{编号}`，可用 `--zero-padding` 补零。
3. **写什么 value** —— string 类型按 `--data-size` 或 `--data-size-range` 决定大小，`--random-data` 决定内容随机还是定值；其它类型写 `--fields` 个字段的定值。
4. **TTL** —— `--expiry-range`（随机，秒）优先于 `--expire`（固定）。

**读命中**：读操作只有在对应 key 已存在时才命中。要测真实读延迟，请：
- 加 `--load` 先把整个 key 空间顺序写满，或
- 让 `--ratio` 含写，先写一段时间预热。

report 会打印 `miss` 比例，miss 高说明读打到了未写入的 key。

**多类型注意**：新 key 格式 `{prefix}{number}` 不再编码类型，多个 `-t` 类型共用同一 key 空间会**撞 key（WRONGTYPE 错误）**。混合多类型时请给不同类型配不同 `--key-prefix`，或一次只压一种类型。

## 任意命令模式（--command）

内置的读写只覆盖 string 的 SET/GET（其它类型内置为纯写）。要压**任意命令 / 任意数据结构的读写**，用 `--command`：模板里用 `__key__`（按分布生成的 key）和 `__data__`（按 `--data-size` 生成的值）占位，多个 `--command` 各带 `--command-ratio` 组成比例。指定 `--command` 后进入命令模式，忽略 `--ratio`/`-t`，report 与 SUMMARY 按命令分别统计延迟。

```bash
# hash HSET:HGET = 2:8
./redis-benchmark-go -a 127.0.0.1:6379 --key-maximum 100000 \
  --command="HSET __key__ f1 __data__" --command-ratio=2 \
  --command="HGET __key__ f1"          --command-ratio=8

# list LPUSH:LINDEX = 2:8
--command="LPUSH __key__ __data__" --command-ratio=2 \
--command="LINDEX __key__ 0"       --command-ratio=8

# set SADD:SISMEMBER = 2:8
--command="SADD __key__ m1"      --command-ratio=2 \
--command="SISMEMBER __key__ m1" --command-ratio=8

# zset ZADD:ZSCORE = 2:8
--command="ZADD __key__ 1 m1" --command-ratio=2 \
--command="ZSCORE __key__ m1" --command-ratio=8

# string SET:GET = 2:8
--command="SET __key__ __data__" --command-ratio=2 \
--command="GET __key__"          --command-ratio=8
```

输出示例（每条命令独立的 count 与延迟分位）：

```
... ops: 99943  HSET_p99: 270µs  HGET_p99: 281µs
SUMMARY-HSET  count: 39780   ... p99: 270µs
SUMMARY-HGET  count: 160220  ... p99: 281µs
```

命中率提示：读能否命中取决于**读的字段/成员是否等于写入的那个**。HGET 固定 field、LINDEX 固定下标、SADD/SISMEMBER 与 ZADD/ZSCORE 用同一个固定成员（如 `m1`）即可稳命中；若读随机 `__data__` 而写的是别的成员，多半 miss。命令模式下 `--load` 被忽略（无法确定如何预热任意命令）。

## 使用示例
```bash
# 1. 纯写 string，均匀 key，64B value
./redis-benchmark-go -a 127.0.0.1:6379 --ops 80000 --data-size 64

# 2. 读写混合 + 热点(zipfian) + 预热
./redis-benchmark-go -a 127.0.0.1:6379 --load \
  --ratio 1:9 --key-pattern Z --key-maximum 1000000 --ops 200000 -c 100 --pipeline 32

# 3. 变长 value + 随机内容 + 随机过期
./redis-benchmark-go -a 127.0.0.1:6379 \
  --data-size-range 16-512 --random-data --expiry-range 60-600 --ops 50000

# 4. 全速压测（测后端上限）
./redis-benchmark-go -a 127.0.0.1:6379 --ops 0 -c 8 --pipeline 32
```

## 预设文件 -P

`-P/--workload FILE` 加载**内部参数**（就是上面这些 flag，key=value 一行一个，`#` 注释）。CLI 上显式给的 flag 会覆盖文件里的值；出现未知参数名会报错。

```ini
# read-heavy.load
ratio=1:9
key-pattern=Z
key-maximum=1000000
data-size=64
pipeline=32
```

```bash
# 用预设，但临时把速率提到 30 万
./redis-benchmark-go -a 127.0.0.1:6379 -P read-heavy.load --load --ops 300000
```

## 针对 Proxy 集群的压测

`-a` 支持多地址，client 按 `client_i -> addrs[i % N]` 轮询均分，把连接分散到不同 proxy。启动时打印分布。

```bash
./redis-benchmark-go \
  -a proxy1:6379,proxy2:6379,proxy3:6379 \
  -c 600 --ops 0 --pipeline 32
# address proxy1:6379: 200 clients ...
```

注意：
- `--ops` 是**本进程全局总速率**，被所有 client 平摊，不是单 proxy 速率。
- `-c` 开到几千时，压测机 `ulimit -n` 要够（每 client 1 conn），跑前先 `ulimit -n 100000`。

## 架构设计

```
[可选] Load 阶段：所有 client 并行、顺序写满 [key-min,key-max]
Run 阶段：N 个 worker（各自独立，无中心分发，无共享限流）:
  [启动] 随机 jitter 错峰
  loop:
    Workload 决定本批 读/写 + 每条的 key(按分布) + value(按大小)
    → redis Pipeline 批量 Exec（计时）
    → 读/写分别计数、记延迟；读检查 miss
    → [可选] 按绝对时刻表节流   ← --ops 0 时跳过
```

- **无单点分发**：每个 worker 自产自销，吞吐随 worker/核数线性扩展。
- **每 worker 绝对时刻表限流**：`ops` 按 client 数均分，节流到绝对时刻 `startTime + opsDone×perOpTickNs`。无共享 limiter、无锁竞争、误差不累积、落后不补偿爆发，比令牌桶更稳。`--ops 0` 全速。
- **启动错峰**：每 worker 起步前随机 sleep 一个相位，避免启动尖峰。
- **Workload 层**：共享的 key 分布生成器（`generator.go`，Uniform/Sequential/Zipfian/ScrambledZipfian）+ 每 worker 私有状态（rand + value 缓冲）。
- **连接可控**：每个 client `PoolSize:1`，总连接数 == `-c`，便于在 proxy 集群上均分。
- **低分配**：worker 复用 `Operation` 与 value 缓冲；非 string 的定值只算一次只读共享；热路径基本只剩 key 字符串一次分配。
- **延迟分位**：读写各一个 **HdrHistogram**（3 位有效数字），按批记录每次 Pipeline Exec 往返延迟。
- **实时报表**：每秒输出 `counter`/`ops`/`avg(5s)`/`write_p99`/`read_p99`；结束打印 `SUMMARY-WRITE` 和（有读时）`SUMMARY-READ`（count/miss%/min/mean/max/p50/p99/p999）。

文件分工：
- `main.go` — cobra CLI、参数解析、`-P` 预设文件、load/run 编排
- `generator.go` — 分布生成器（Uniform/Sequential/Zipfian/ScrambledZipfian）
- `workload.go` — Workload（key 分布 + 读写比 + value 大小 + TTL）与每 worker 状态
- `command.go` — 任意命令模式：commandSpec（模板/占位符替换/加权）、valueSizer
- `data_generator.go` — 类型定义、`IsSupportedType`、非 string 的定值 payload
- `sender.go` — 多 worker 发送、时刻表限流、读写 pipeline、命中统计、报表、load 阶段
- `histogram.go` — 封装 HdrHistogram（加锁，按批记录），提供 p50/p99/p999 与 min/mean/max

## 性能与调优经验

在 48 核机器 + 本地单实例 Redis 上实测：纯写全速 ~55~65 万 ops/s；redis-server ~99%（单线程打满 1 核），压测端 ~1.4 核 / 8 worker、内存 ~45 MB。

结论与建议：
- **瓶颈通常在后端（Redis/Proxy），不在压测工具**。单实例 ~56 万 ops/核 基本是天花板。
- 冲几十万~几百万 QPS 靠**集群多分片并行 + 多地址分发**，而非堆单实例。
- 加大 `--pipeline` 提升单连接效率、摊薄网络/系统调用开销，但**尾延迟会上升**，50~100 是压吞吐常用区间。
- 测**读**延迟务必先 `--load` 预热，否则 miss 高会失真。
- 测**热点**用 `--key-pattern Z`；缩小 `--key-maximum` 也能提高命中与热点集中度。

## 测试

```bash
# 仅单元测试（无需 Redis：分布/类型校验，e2e 用例会自动 Skip）
go test ./...

# 端到端测试：指向一个可用 Redis
REDIS_ADDR=127.0.0.1:6379 go test -race ./...
```

- `generator_test.go`：分布正确性（uniform 范围/均匀、sequential 循环、zipfian/scrambled 集中度），无依赖。
- `data_generator_test.go`：`IsSupportedType` 与各类型 `typedValue`，无依赖。
- `sender_test.go`：连真实 Redis，覆盖 string 写入与 value 大小、各类型写入、TTL、`--data-size-range`、`--load` + 读写混合（验证读零 miss）、全速/限流；**连不上则 `t.Skip`**。
- 每个 e2e 用例用带时间戳的唯一 `key-prefix`，只 `SCAN`+`DEL` 自己的 key，不 FLUSHDB。


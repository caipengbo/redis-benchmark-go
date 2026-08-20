# async fork 对 Redis fork 延迟影响 —— 测试方案

> 目标:在阿里云 alinux 主机上验证内核 async fork 特性对 Redis fork 延迟的影响。
> 主指标:`latest_fork_usec`(fork 调用阻塞主线程的微秒数)。
> 对比维度:数据集 **1G/10G/20G/30G/40G/50G × async fork on/off**(6 档 × 2 = 12 组)。
> 目标主机:`112.126.80.162`(alinux 4,内核 `6.6.102-7.alnx4`,61G 内存无 swap,磁盘 91G 可用)。

## 〇、机制与预期基准(据 Async-fork VLDB 2023 论文,阿里+上交团队)

- **默认 fork 逐级同步拷贝整张页表**(PGD→PUD→PMD→PTE),阻塞主线程;页表拷贝占 fork 总耗时 **>97%(64GB 时 99.93%)**,fork 时间**随内存近似线性**:论文 1GB <10ms,64GB >600ms。
- **Async-fork**:父进程只用 "Fast" 拷顶层 PGD/PUD 并把 PMD 设写保护后**立即返回**;PMD/PTE 由子进程被调度后异步 "Slow" 拷。父进程在子进程拷完前若要改未拷的 PMD/PTE,触发一次"主动同步"(先拷再改)——**这是写期间的退化路径**。→ 静止态(本方案 Q3)几乎无主动同步,async-fork 处于最佳状态,对比最干净。
- **论文降幅基准(fork 致 p99)**:1GB 17.57%、8GB 81.76%、64GB **99.84%**(991.9ms→1.5ms)。即低内存收益小、高内存收益巨大,可作我方结果的量级对照。

## 一、结论先行(判定标准)

- **预期**:async fork **on** 的 `latest_fork_usec` 显著低于 **off**,且**差距随内存增大而拉大**(off 近似线性正比于页表/内存,on 近似平坦或增长缓慢)。
- **若 on/off 无明显差异**:要么开关未真正生效(回查 memcg `memory.async_fork`),要么该路径未覆盖——也是有价值的结论。

## 二、核心决策(已确认)

| # | 决策项 | 选定 |
|---|--------|------|
| 1 | 主指标 | `latest_fork_usec`(只测 fork 调用本身耗时,不测客户端尖刺) |
| 2 | fork 触发方式 | 只用 `redis-cli BGSAVE` 手动触发 |
| 3 | 数据状态 | **静止态**:灌满 → 停写 → 反复 BGSAVE |
| 3 | 采样次数 | 每档 10 次,丢第 1 次(预热),取后 9 次 |
| 4 | 数据结构 | string,`--data-size 1024`(1KB 定长),`--key-pattern S` 顺序灌 |
| 4 | 横轴刻度 | `used_memory`(非进程 RSS)达标为准,14 组统一 value size |
| 5 | 开关方式 | 同一台机器重测;每档内「关→开」紧邻对比 |
| 6 | 采样纪律 | 轮询 `rdb_bgsave_in_progress`→0、记录、固定 sleep 3s |
| 6 | THP | 全程统一 `never`,只作受控变量记录,不进对比矩阵 |
| 7 | 档位推进 | **增量累加**:1G→10G→…→60G 只补灌差量 |
| 7 | Redis 配置 | `save ''` + `appendonly no` + 不设 maxmemory,单机独占 |
| 8 | 执行编排 | 单档内「先 off×10 → 后 on×10」紧邻循环,再补灌进下一档 |
| 9 | 产出物 | CSV 原始记录 + 环境存档 + median 双曲线主图 + 下降比例辅表 |
| 10 | 自动化 | 半自动:采样内循环用 bash 脚本,灌数据用本工具,开关/档位手动确认 |

## 三、待核对项(拿到主机后确认)

1. **内核 async fork 开关**:`uname -r` + 查 alinux 文档,确认开关名与生效方式(sysctl 动态 / boot 参数 / `/sys` 接口)。
   - 若**动态可切换**(推荐路径):按下方单档循环执行。
   - 若**必须重启生效**:退回「两大轮」——off 跑完 1G→60G 全程增量,重启开 on,重灌再跑一遍全程。
2. **机器规格**:需 **≥96G 内存**(60G 数据 + 系统/余量)。内存不足则下调最高档。
3. Redis 版本、CPU/NUMA、磁盘型号——存档备查。

## 四、环境准备

```bash
# 1. THP 关闭(两组一致)
echo never > /sys/kernel/mm/transparent_hugepage/enabled
cat /sys/kernel/mm/transparent_hugepage/enabled          # 存档

# 2. 启动 Redis(单机独占)
redis-server --save '' --appendonly no --daemonize yes --dir /data/redis
# 不设 maxmemory,靠 key 数控制 used_memory

# 3. 环境存档
uname -r
redis-cli INFO server | grep redis_version
free -g; lscpu | grep -E 'NUMA|Socket|Core'
```

## 五、灌数据(用本 benchmark 工具)

- value=1KB → 1G ≈ 100 万 key,60G ≈ 6000 万 key。
- 顺序写、全速,补灌到目标 `used_memory` 即停:

```bash
# 示例:补灌到某档,--key-minimum/--key-maximum 控制本次补灌的 key 区间,
# 顺序(S)写保证不重复;写完后 redis-cli INFO memory 看 used_memory 是否达标。
./redis-benchmark-go -a 127.0.0.1:6379 \
  --ops 0 -c 16 --pipeline 64 \
  --key-pattern S --data-size 1024 -t string \
  --key-minimum <上一档末尾+1> --key-maximum <本档目标末尾> \
  -d <足够长时间>
```

> 灌完每档后**停止写入**,再进入采样。用 `INFO memory` 的 `used_memory_human` 确认达标。

## 六、采样脚本(半自动)

`sample_fork.sh` —— 采样内循环(每档、每个 on/off 状态各调用一次):

```bash
#!/usr/bin/env bash
# 用法: ./sample_fork.sh <redis-host:port> <mem_label> <asyncfork_state> <out.csv>
set -euo pipefail
HOSTPORT=$1; MEM=$2; STATE=$3; CSV=$4
CLI="redis-cli -h ${HOSTPORT%:*} -p ${HOSTPORT#*:}"

for i in $(seq 1 10); do
  # 等待上一次 BGSAVE 子进程退出
  while [ "$($CLI INFO persistence | tr -d '\r' | awk -F: '/rdb_bgsave_in_progress/{print $2}')" != "0" ]; do
    sleep 0.5
  done
  $CLI BGSAVE >/dev/null
  # 等本次 BGSAVE 完成后再读 latest_fork_usec(fork 已发生,值已刷新)
  while [ "$($CLI INFO persistence | tr -d '\r' | awk -F: '/rdb_bgsave_in_progress/{print $2}')" != "0" ]; do
    sleep 0.2
  done
  USEC=$($CLI INFO stats | tr -d '\r' | awk -F: '/latest_fork_usec/{print $2}')
  USED=$($CLI INFO memory | tr -d '\r' | awk -F: '/used_memory:/{print $2}')
  TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "${MEM},${USED},${STATE},${i},${USEC},${TS}" >> "$CSV"
  sleep 3
done
```

CSV 表头:`mem_label,used_memory_bytes,async_fork,round,latest_fork_usec,timestamp`
> 分析时**丢弃每组 round=1**(冷启动预热),取 round 2~10 共 9 个样本。

## 七、单档执行循环(动态可切换假设)

对每个内存档位 XG(数据已累加到位、已停写):

```
1. 设 async fork = off   →  ./sample_fork.sh host:port 10G off  results.csv
2. 设 async fork = on    →  ./sample_fork.sh host:port 10G on   results.csv
3. 补灌差量到下一档,回到 1
```

> 全档固定「先 off 后 on」,消除顺序偏差。async fork 开关切换与档位推进手动确认(灌到位、开关生效各人工核对一次)。

## 八、结果呈现

- 每(档位 × on/off)取 round 2~10 → **min / median / p99 / max**。
- **主图**:横轴 `used_memory`(1→60G),纵轴 `latest_fork_usec`(median),两条线 on/off。
- **辅表**:每档 off/on median + 下降比例 `(off-on)/off`;附 p99/max 反映尾部尖刺。

## 九、风险与注意

- **磁盘慢**:60G dump 到盘会拉长子进程存活时间,但**不影响 `latest_fork_usec`**(该指标只计 fork 调用本身)。仅影响「多久能触发下一次」——靠轮询 `rdb_bgsave_in_progress` 规避。
- **多类型撞 key**:本方案只用 string、单一 key-prefix、顺序写,无撞 key 风险。
- **重启生效退路**:若 async fork 必须重启生效,增量累加+紧邻对比不成立,改「两大轮」(各自从零增量累加),重灌成本翻倍。

# extsort — 本地外排作业运行器 + 共享缓冲仲裁器

同机数据整备场景：多份真实 JSONL 排序/去重作业在**同一进程**内并发推进，
共享一个硬内存预算 `B`，由仲裁器（`BufferArbiter`）统一记账、准入、回收。
不做跨日调度、DAG 检查点或数据版本发布——只管运行时缓冲。

```
extsort/
  arbiter.py    共享缓冲仲裁器：预算、预留、准入、回收、取消唤醒
  job.py        外排作业：读入→溢写→多路归并→原子发布输出
  ioengine.py   每作业一个写线程：异步落盘、tmp→rename 发布、迟到回调处理
  runner.py     submit / status / cancel / wait 接口
  verify.py     独立校验器（不与排序器共享代码）
  cli.py        一次运行多份文件的命令
tests/          全部测试（纯 python 脚本，无需 pytest）
artifacts/      大文件测试的完整事件日志、输出首尾、报告
```

## 核算口径（什么算进 B）

`B` 只覆盖**驻留的字节内容**，按对象精确记账，一个 `bytes`/`bytearray`
对象无论被引用多少次只计一次：

| kind         | 内容                                                     |
|--------------|----------------------------------------------------------|
| `line`       | 读入的原始行字节（读前预留块上限，读后 adjust 到实际）    |
| `key`        | 排序键的 UTF-8 编码字节（编码前按 4B/字符上限预留，后调整）|
| `merge-read` | 归并读缓冲块、跨块记录的精确余量、16B 头部暂存            |
| `merge-key`  | 归并时物化的键（每块一次预留，用于堆比较）                |
| `buffer`     | 序列化/输出写缓冲（每作业 2 个，循环复用）                |
| `io`         | 实际在途写盘数据（写缓冲 transfer 而来，同一对象同一预留）|

规则与入口：

- **分配入口唯一**：`BufferArbiter.reserve()`，结果只有 grant / wait / fail
  （`AllocFailed`）三种；`release/adjust/transfer` 是仅有的其他记账操作。
- **复制必须先预留再复制**：`readline(limit+2)` 之前先预留块上限；UTF-8 编码
  前先按 4B/字符上限预留；序列化用 `struct.pack_into` + 切片赋值写入已预留
  缓冲（零临时拷贝）；冲刷时缓冲对象本身移交写线程（`transfer` 到 `io`，
  同一对象、同一预留，不产生新拷贝）。
- **磁盘文件不计驻留预算**（溢写 run、输出文件）。
- **Python 对象固定元数据开销另列**：list 槽、tuple/dict 头、JSON 解析的
  临时 str/dict、timsort 的指针暂存等不进 `B`，由
  `report()["python_metadata_estimate_bytes"]` 单独估算；同时报告
  `peak_rss_bytes`（`/proc/self/status` 的 VmHWM 内核高水位，真实峰值
  RSS）。我们不声称 `B == 进程 RSS`。
- 仲裁器 debug 模式在**每次记账变更后**断言
  `total == Σ预留 == Σ按kind统计 ≤ B`；每条仲裁事件都带 `total_reserved`
  快照，测试对事件日志**逐事件**重放恒等式。

## 最小工作集（MWS）准入与无等待环

每个作业提交时计算 MWS（超出 `B` 直接拒绝）：

```
max_record  = 16(头) + 5*max_line        # key ≤ 4×行字节(UTF-8)
read_mws    = read_block + 4*read_block + 2*write_buf
merge_mws   = fan_in*(2*merge_chunk + max_record + 16) + 2*write_buf
mws         = max(read_mws, merge_mws)
```

- **包络准入**：作业每个阶段开始时 `begin_phase` 申请包络，FIFO 排队，
  不变式 `Σ活跃包络 ≤ B` 恒成立（排队者持有 0 字节）。
- **包络内预留可无限等待**（不会发生等待环，见下）；**包络外预留必须
  fail-fast**（`timeout=0`：先做一轮协作回收，仍不够立即 `AllocFailed`），
  调用方回退为**自我溢写**——溢写不需要任何额外内存（行/键字节已预留，
  写缓冲在包络内），因此任何持有 ≥ MWS 的作业总能推进。
- **无等待环论证**：等待中的作业必有 `held + req ≤ 包络`；若全部作业都在
  等，则 `Σreq ≤ B − Σheld = free`，与每个 `req > free` 矛盾（包络外持有
  均为可撤回数据，回收即可释放）。所以永远有人能推进；包络外请求失败即
  自我溢写，也不存在"占着缓冲等别人"的循环。
- **公平性**：包络 FIFO 准入；作业在阶段边界释放包络；读阶段每次溢写都
  释放行/键预留，给其他作业留出物理空间。大文件测试中 8 个短作业全部在
  长作业运行期间完成（`shorts_during_long=8`），没有任何作业被永久挂起。

## 协作回收回调（reclaim callback）

`reserve` 发现不足时，先在**不持仲裁器锁**的情况下调用受害作业的
`_reclaim_handler`（在请求方线程上执行）：

- **可重入性**：回调**绝不调用 `reserve`**，只做 `release/transfer` 和
  作业内部条件变量等待，因此不存在重入死锁；回收轮次由
  `reclaim_lock` 串行化，同一作业的回调不会并发执行；回调经
  `run_lock`/`spill_lock` 与受害作业的工作线程同步，受害作业取消时回调
  提前返回（已释放的内存保持已释放）。
- **何时允许释放**：每个 reservation 有唯一逻辑所有者，释放恰好一次；
  在途 I/O 的预留由写线程回调释放；作业清理时先 `writer.join()` 再做
  `release_all` 兜底，因此迟到回调不会重复释放。`release` 幂等防御，
  二次释放会产生 `DOUBLE_RELEASE` 事件——所有测试断言该事件为 0。
- **不持锁等 I/O**：锁序固定为
  `run_lock → arbiter锁 → pool/writer条件变量`；仲裁器锁内从不等待 I/O；
  回收回调在锁外执行；写线程完成回调只顺序取锁、不嵌套。溢写序列化完成
  即释放行/键预留（不等写盘完成），回收效果立即可见。

## 取消语义（cancel(job_id)）

取消在三个位置都完整处理：

1. **等待额度中**：`cancel_job` 唤醒该作业所有等待，waiter 重查取消集
   抛 `Cancelled`（事件 `RESERVE_ABORT`）。
2. **回收回调执行中**：回调检查取消标志后提前返回（已释放内存不回滚）；
   作业自身在下一检查点退出。
3. **写盘已发出未确认**：在途写由写线程完成（系统调用不可中断），迟到
   的完成回调**不重新拿额度、不发布结果**——tmp 文件删除、预留照常释放
   一次；输出采用 `tmp → os.replace` 原子发布，取消/失败永不出现半成品。

作业终态（完成/失败/取消）时：reservation 清零、`writer.join()` 后兜底
释放、只删除自己的临时目录与输出 tmp；真实磁盘写失败（含注入的
`OSError`）只失败该作业，仲裁器状态继续可用。

## 接口与命令

```python
runner = JobRunner(budget_bytes=64<<20, work_dir="/tmp/esx",
                   event_log_path="/tmp/esx/events.jsonl")
jid = runner.submit("a.jsonl", "out/a.sorted",
                    max_line_bytes=1<<18, dedup_key=False)  # MWS>B 时抛 ConfigError
runner.status(jid)      # state/phase/lines/held_bytes/mws/...
runner.cancel(jid)
runner.wait(jid, timeout=None)   # 终态 status
runner.report()         # high_water、kind 峰值、元数据估算、真实峰值 RSS
```

一次运行多份文件（同进程并发、共享预算）：

```bash
python -m extsort run --budget 64MB --workdir /tmp/esx -o /tmp/out \
    --max-line-bytes 256KB --dedup a.jsonl b.jsonl c.jsonl
python -m extsort verify --input a.jsonl --output /tmp/out/a.jsonl.sorted --dedup
python -m extsort verify --input a.jsonl --output /tmp/out/a.jsonl.sorted --exact --dedup
```

排序语义：按 `text` 键**码点升序**（UTF-8 字节序 ≡ 码点序）、原行号升序
稳定；`dedup_key` 模式每键只保留最早原行（归并末趟按键相邻去重）。
输入契约：每行一个 JSON 对象、含非空 `text` 字符串；超 `max_line_bytes`
的行使该作业失败（不截断、不发布部分输出）；空输入产生空输出。

## 测试（`bash tests/run_all.sh`，全部真实执行）

| 测试 | 内容 |
|------|------|
| `test_barrier_reclaim.py` | `threading.Barrier` 让 A/B 各持约 40% 预算后同时申请额外写缓冲：协作回收、真实溢写、恰好一方等待并被对方阶段释放唤醒；逐事件恒等式 |
| `test_cancel_paths.py`    | 取消分别发生在等待额度、回收回调中、在途写盘中；A 取消后 B/C 真实完成并与内存 `sorted()` 逐字节对照；迟到回调无复活、无重复释放 |
| `test_io_failure.py`      | 注入真实 `OSError` 写失败：只失败该作业，仲裁器继续可用，后续作业正常 |
| `test_edges.py`           | 空输入、超长行（失败不截断）、热点重复键 dedup 多趟归并、扇入>缓冲多趟归并、MWS 超预算提交即拒、Unicode 码点序+稳定性 |
| `test_large.py`           | 运行时生成 86.5MB（4.6×B=18MB），2 长作业 + 8 短作业流式提交；全部完成；独立流式校验（有序/orig 稳定/行数/SHA-256 指纹）；完整事件日志、输出首尾、报告落盘 `artifacts/` |

大文件测试实测（`artifacts/large-*/report.json`）：

```
total_input 86.5MB vs budget 18.9MB (4.6x)   elapsed ≈ 3.7s
high_water 18,783,913 / 18,874,368 B          # 预算真的被顶到 99.5%，从未越界
admit_waits=20  reclaims=11  spills=21        # 准入排队与协作回收真实发生
shorts_during_long=8                          # 短作业全部在长作业运行期完成
peak_rss ≈ 52MB（VmHWM 真实峰值；B=18MB 只计驻留字节内容）
事件日志 6081 条，逐条重放 total_reserved ≤ budget：0 违规；DOUBLE_RELEASE=0
```

说明：在当前几何下，同步协作回收几乎总能即时解围，所以大文件测试中
`RESERVE_WAIT` 稀少；等待/唤醒/路径由屏障测试与取消测试确定性覆盖。

---

# 时间范围全外连接（interval-join）

`extsort.interval.IntervalJoinJob` 在同一套外排内核、同一个 `BufferArbiter`
之上实现"观测值关联有效时间段"的全外连接，使用者**不需要先排序**，直接从
两份乱序 JSONL 得到可追溯明细。提交入口与普通排序共用一个 runner：

```python
jid = runner.submit_interval_join(left_path, right_path, output_path, ...)
runner.status(jid)      # 同一套 status/cancel/wait，返回四阶段与各项计数
runner.cancel(jid); runner.wait(jid)
```

CLI：

```bash
python -m extsort.cli interval-join --left L.jsonl --right R.jsonl \
    -o out.jsonl --budget 64MB --workdir ./work [--max-line-bytes ...]
```

## 数据合同

* 左表每行：非空字符串 `text`、真有符号 64 位整数 `start`、`end`，
  `start <= end`（`start == end` 是空区间，合法但永不命中）；
* 右表每行：非空字符串 `text`、真有符号 64 位整数 `at`；
* `bool`、浮点、越界整数、缺失/非字符串 `text`、空行、非法 JSON、超长行都让
  作业失败，错误明确给出**侧别（left/right）与从 1 开始的物理行号**
  （`IntervalInputError`），不发布半成品；
* 相同内容的两行仍是两个不同来源行，不去重；
* 命中条件仅为 `text 完全相同 and start <= at < end`（半开区间，不做时区转换）。

## 输出合同

每行 `kind/key/left_row/right_row/left/right`：`match` 保留两侧原对象与两个
物理行号；`left_only`/`right_only` 缺侧为 `null`。顺序固定为 key 的 Unicode
码点序；同 key 内所有带左行的记录（match、left_only）按 left_row 升序、同
left_row 的 match 再按 right_row 升序，全部先于该 key 的 right_only（按
right_row 升序）。因此换读块大小、溢写次数、线程交错都不改变输出。

## 四阶段与磁盘组织

每个阶段都在自己的 MWS 包络内准入，`status().phase` 依次为
`organize -> join -> output_sort -> publish`，失败/取消终结，绝不发布。

1. **organize（读取整理）**：逐块校验、预留后编码，按
   `(key, start, lineno)` / `(key, at, lineno)` 溢写排序 run，再多趟 k-way
   归并成全局有序的 `left.side` / `right.side`（定长二进制头 + key + 原始行）。
2. **join（关联）**：两侧按键归并进入同一 key 的扫描。右观测按 `at` 升序，
   左区间用排序游标把 `start <= at` 者装入按 `end` 排序的活跃小顶堆，
   `end <= at` 者过期；堆字节超阈值时整体按 end 序溢成 `active-*.run`，
   活跃 run 带持久过期 mark，后续观测只做后缀重扫，run 数到上限再 k-way 合并。
   命中的左行序号写入分页位图 `marker-*.bit`（驻留页数有界，脏页回写），
   每条结果（含两侧原始行）滚动写入 `result-*.run`。key 结束后按 marker 流式
   重读左组，未标记的发 `left_only`；未命中的观测即时发 `right_only`。
3. **output_sort（输出重排）**：result run 是扫描序而非输出序，先对每个 run
   做有界块排序生成有序 run，再多趟 k-way 归并，保证最终来源行顺序。
4. **publish（发布）**：渲染 JSONL 到 `output.tmp.<jid>`，`os.replace` 原子
   替换；取消/失败只清本作业临时目录，保留输入与既有目标文件。

## 候选检查口径与稀疏线性界

候选检查只在两个方法计数，没有任何比较藏在辅助函数里：`_check_start`（左
记录对某 `at` 的 start 判定）与 `_evaluate`（完整 `start <= at < end`）。
活跃 run 后缀重扫的每次判定同样计数。对"N 个互不相交区间、每区间恰一个观测"
的稀疏形态，每区间只承受常数次 start/过期/命中检查，
`candidate_checks` 为 O(N)（测试断言 `<= 8N`，合同验收界为 `<= 64N`），
而不是 N² 全组合；稠密重叠允许与命中乘积同阶，不截断不抽样。

`status()` 暴露 `candidate_checks`、`active_flushes`、`active_merges`、
`marker_pages`、`bytes_written`/`bytes_read`（含位图/索引页 I/O）及
matches/left_only/right_only 与两侧输入数。

## 内存有界性

两侧原始文件、单 key 时间段组、仍活跃区间集、匹配中间结果、最终配对输出都
可能分别大于预算 B，且全部走临时磁盘：行/键/时间索引编码页、匹配标记位图页、
活跃区间行（kind `active`）、在途读写（`io`/`buffer`/`merge-read` 等）都先
预留后复制，最后一个产品引用消失后才归还，逐事件满足
`total_reserved <= budget`；Python 固定对象开销仍按既有口径单独估算。所有
内部整理/关联预留都登记在**同一个作业 id** 下，不存在独立子作业，因此内部
任务不会等待自己占住的额度而死锁；协作回收回调只溢写 organize 阶段已预留的
run，回调内绝不调用 reserve。

## 测试

| 测试 | 内容 |
|------|------|
| `test_interval_correctness.py` | 手算例逐条核对来源行与顺序；40 个随机种子 × 3 种缓冲几何，对独立双重循环参考实现（`ij_ref.py`，不用生产匹配/排序函数）逐记录比对；空文件/单侧键；稀疏 `<=64N` 候选界；稠密活跃集溢盘 |
| `test_interval_validation.py`  | 空行、非法 JSON、bool/浮点/越界整数、缺字段、空 text、start>end、超长行、迟到非法记录；侧别+行号错误、无半成品、仲裁器可复用、故障隔离 |
| `test_interval_lifecycle.py`   | 两个确定性取消点（热点组已溢盘、配对已部分生成未发布）的 hook 闸门；注入写失败只伤本作业；与普通排序作业在同一仲裁器下并发完成；逐事件预算恒等式、额度归零 |
| `test_interval_scale.py`       | 运行时造数：单 key 两侧组 > B 的稀疏关联、活跃集与配对乘积 > B 的稠密关联；流式顺序/来源校验、候选界、I/O 与峰值 RSS、预留事件轨迹与字节对象生命周期 |

生成的大数据与输出只落在临时/`artifacts` 目录，夹具与生成器在 `tests/` 内，
不提交大文件。


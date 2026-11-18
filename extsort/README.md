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

---

# 分组精确分位阈值（group-quantiles）

`extsort.quantiles.GroupQuantileJob` 在同一套外排内核与同一个
`BufferArbiter` 之上，从**未排序**的测量 JSONL 直接给出每个 `text` 组的
多个**精确** nearest-rank 分位阈值及其来源物理行——产出是阈值结果，而不是
一份重新排序的全量明细。提交入口与普通排序共用一个 runner：

```python
jid = runner.submit_group_quantiles("meas.jsonl", "out/quant.jsonl",
                                    [0, 2500, 5000, 10000])   # bp: 0..10000
runner.status(jid)      # 同一套 status/cancel/wait
runner.cancel(jid); runner.wait(jid)
```

CLI：

```bash
python -m extsort.cli group-quantiles --input meas.jsonl -o quant.jsonl \
    --bp 0,2500,5000,10000 --budget 64MB --workdir ./work
```

## 精确十进制语义（禁止 binary float）

`value` 按 JSON 十进制数值精确比较，整数与小数一视同仁，**全程不转
binary float**：JSON 解码用 parse hook 保留原始数字词法（lexeme），纯字符串
运算把词法规范化成"字节序即数值序"的定长前缀顺序键
（`decimal_order_key`：符号类 neg<zero<pos、2 字节偏置十进制幂 lead、
终止符感知的有效数字串，负数整体反向）。`1`、`1.0`、`1e0` 同值同键；超过
binary-float 精度的相邻整数仍可区分；极小/极大指数（受行宽约束）精确可比。
原始拼写逐字节保留为结果里的 `value_token`。`bool/null/字符串/NaN/Infinity/
缺字段/空行/非法 JSON/超 max_line_bytes` 都让整项作业失败，错误给出从 1
开始的**物理行号与原因**，绝不跳过坏行。

## 排名与输出合同

每组 n 条有效记录，对每个请求基点 `bp`（一次作业内互不重复的整数，
0..10000，10000=100%）按 nearest-rank 计算
`r = max(1, ceil(bp*n/10000))`，排名从 1 开始；`bp=0` 取最小值。目标排名
落在重复数值上时，来源取该**数值运行（equal-value run）中物理行号最小**的
记录及其原始拼写。输出每行一个 JSON 对象，字段
`text,bp,n,rank,source_line,value_token`，按 text 的 Unicode 码点序、再按
bp 数值序排列；空输入产生空结果。因此改变读块、run 容量、归并扇入都不改变
输出字节。

## 一次整理、多 bp 共用；热点组与海量组都不驻留

阶段（各自在 MWS 包络内准入，`status().phase` 可观察
`parse -> merge -> extract -> publish`）：

1. **parse（解析/溢写）**：逐块校验、先预留后复制，按
   `(text, 数值顺序键, 物理行号)` 溢写排序 run。所有 bp 共用这**一次**输入
   解析与外排，bp 数量不改变溢写/归并工作量。
2. **merge（归并）**：超扇入时多趟 k-way 归并；末趟在同一次扫描里写出全局
   有序 run 和一份流式的分组索引 `(key, n)`——任何时刻都不保留整组数值，也
   不保留全量分组计数表。单一热点组可以远大于 B，组数量也可以远大于 B。
3. **extract（排名提取）**：按索引逐组流式扫描有序 run，只暂存命中目标排名
   的等值运行（每组至多一个等值运行的 token 驻留），内存与组大小无关。
4. **publish（发布）**：渲染 JSONL 到 `output.tmp.<jid>`，`os.replace`
   原子替换；解析/归并/写盘失败或取消时不发布前缀，保留原目标、输入与其他
   作业文件，只清理本作业临时目录并归还全部预留。

数值解析表示、排序 run、归并读写、结果缓冲与在途 I/O 字节全部登记在
**同一个 BufferArbiter 的 B 口径**（新增 kind `value`/`vkey`，复用
`line/key/merge-read/merge-key/buffer/io`）；复制前先预留，任意时刻总预留
≤ B，Python 固定元数据与真实峰值 RSS（VmHWM）分开报告。所有内部预留挂在
同一个作业 id 下，包络外请求 fail-fast 回退为自我溢写，内部阶段不会等待
自己占住的额度而死锁；协作回收回调只溢写 parse 阶段已预留的 run，回调内
绝不调用 reserve。本功能**不增加**崩溃后恢复协议。

## 测试

| 测试 | 内容 |
|------|------|
| `test_gq_correctness.py` | 独立 `Fraction` 参考计算器（`gq_ref.py`，不 import 任何产品解析/比较/排序代码）：负数、整数小数混写、超 float 精度相邻整数、极小/极大指数、等值异写、重复值、Unicode text、bp=0/10000、多 bp 同排名、空输入；4 种读块/run/扇入几何输出逐字节一致；来源行与完整顺序核对 |
| `test_gq_validation.py`  | bool/null/字符串值、NaN/Infinity、缺字段、空 text、空行、非法 JSON、超长行、后段非法值；物理行号错误、无半成品、旧目标字节不变、兄弟作业与仲裁器不受影响 |
| `test_gq_lifecycle.py`   | parse/extract/准入等待三处可控取消、真实注入写失败（旧目标不变）、与普通排序并发完成、阶段与计数/预算可观察性、额度归零 |
| `test_gq_scale.py`       | 运行时造数：大于 B 的单一热点组、8000 个小组多趟归并；逐事件重放预留不越界、流式核对已知排名/来源/行数、证明多 bp 共用一次整理、RSS 与预算分别记录 |

大数据、临时 run 与输出只在运行时生成于临时目录，不提交到 Git。

---

# 可跨进程接续的排序任务（resumable sort）

`extsort.resumable` 在同一套外排内核与同一个 `BufferArbiter` 之上提供带
**持久进度**的 sort/dedup：进程被杀（含 SIGKILL）后，重新启动 Python、
指定原任务的状态目录即可接着尚未做完的部分运行——不重读、不解析、不重排
已确认前缀的原始内容，也不重跑已确认的归并组。排序语义与普通排序完全
一致：`text` 非空、按 Unicode 码点升序、同键按原始物理行号稳定；dedup
保留整份输入中该键最早的一行；保留行原始字节，末行无换行时交付前补一
个换行（相邻记录不会被粘在一起）；空输入产生空输出；非法行/超长行使任
务失败，绝不静默跳过。

```python
jid = runner.submit_resumable_sort("big.jsonl", "out/big.sorted",
                                   "/srv/esx/state-big",
                                   dedup_key=True, max_line_bytes=1<<18,
                                   checkpoint_input_bytes=64<<20)
runner.status(jid); runner.cancel(jid); runner.wait(jid)   # 同一套管理接口

# 进程重启后（新进程、新 runner，预算和块/扇入可以不同）：
jid2 = runner2.resume_sort("/srv/esx/state-big",
                           read_block_bytes=1<<20, max_fan_in=8)
```

CLI（退出码 0=DONE 1=FAILED 3=拒绝 4=可恢复 I/O 错误 5=CANCELLED）：

```bash
python -m extsort rsort-new --input big.jsonl --output out/big.sorted \
    --state-dir /srv/esx/state-big --budget 64MB --dedup \
    --checkpoint-input-bytes 64MB
python -m extsort rsort-status --state-dir /srv/esx/state-big [--verify-runs]
python -m extsort rsort-resume --state-dir /srv/esx/state-big --budget 32MB \
    --max-fan-in 8
```

## 状态目录（任务的私有工作区，崩溃后保留）

```
state_dir/
  LOCK            flock(LOCK_EX|LOCK_NB) 持有文件；内核在持有者死亡时自动
                  释放——不靠 PID 文件或超时推断存活。内容仅为诊断。
  task.json       创建时写一次，之后只读：task_id、输入绑定（规范路径、
                  dev/ino/size/mtime_ns/ctime_ns）、创建时观察到的目标文件
                  身份（或不存在）、排序语义（dedup_key、max_line_bytes）。
  progress.json   持久状态机：generation（每次检查点 +1）、phase
                  (read/merge/publish/done)、read{confirmed_offset,
                  next_lineno, eof}、确认 run 列表（名称/记录数/字节数/
                  sha256）、run/merge 序号计数器、publish 记录、stopped
                  终态标记、总量与按尝试的工作量统计。每次更新都是
                  写 tmp → fsync(文件) → os.replace → fsync(目录)。
  runs/           run-NNNNNN.run / merge-NNNNNN.run（.tmp→fsync→rename）。
  events.jsonl    各尝试追加的事件日志（CLI 默认位置）。
```

新建要求状态目录不存在或为空（仅容忍 runner 自己的 events.jsonl）；
state_dir 不得是输入/输出文件或其所在目录（创建时拒绝，清理因此永远碰
不到输入或别的任务的状态）。同一目录同一时刻只能有一个写入者：第二个
进程的新建/恢复立即以 `StateLocked` 拒绝。

## 协议

**读入检查点**：溢写 run 先落盘（tmp→fsync→rename），随后 `wait_idle`
+ fsync(runs/)，最后才把 `{confirmed_offset, next_lineno}` 与完整确认
run 列表写进 progress.json——"读到这里"与"这些行已安全留在磁盘"严格分
开：崩溃时未被检查点覆盖的溢写 run 是孤儿，恢复时删除、对应尾段重读
（重读量 ≤ 一个检查点间隔 + 一个读块 + 一条行）。`checkpoint_input_bytes`
可配：未确认前缀超过阈值后，至多再读完一条合法行就同步发起检查点，落
盘确认前不再读新数据；绝不会等整份输入读完才第一次保存。恢复时用
`seek(confirmed_offset)` 打开来源，已确认前缀不再读取（事件
`RSORT_SOURCE_OPEN/RSORT_SOURCE_BLOCK` 记录每次真实打开/读取的范围，
测试据此逐断言）。

**归并接续**：确定式队列计划——每次取队首 `max_fan_in` 个确认 run 归并
为 `merge-NNNNNN.run`，fsync 后先持久化新 run 列表（输入移除、输出追
加、generation+1），再回收被替代的输入文件。崩溃点在持久化之前→孤儿
输出删除、该组重做；在持久化之后、回收之前→旧输入作为孤儿删除，组不
重跑。已确认组永不重跑；未做完的单个组与未完成的最终输出允许重做
（重做边界：每次崩溃至多 1 个未确认组 + 1 趟最终输出）。最终字节与
run 切分/恢复次数无关（堆序 (key, lineno) 的全序归并 + dedup 取最早行
都是分组无关的）。

**发布**：最终归并写 `<output>.tmp.<task_id>.part` → rename 为
`<output>.tmp.<task_id>`（fsync）→ 持久化 phase=publish（含暂存文件
的字节数与 sha256）→ 重新核对目标仍与创建时观察一致 → 一次
`os.replace` 原子替换 → fsync(父目录) → 持久化 phase=done。恢复时：
目标已持有本任务结果（哈希一致）→ 只补记 done（"已替换未记录"窗口）；
目标仍是当初观察的旧文件且暂存完好 → 重新 rename；暂存损坏 → 重做最
终归并；目标是别的内容 → `PublishConflict`，保留现场、不覆盖、不认
成功。DONE 任务的恢复同样核验目标：仍在则幂等返回（不重读输入、不重
做归并），缺失或被更换则报告问题且不改写持久终态。

**终态**：`cancel` 是明确放弃——持久化 `stopped=cancelled` 后恢复只回
显终态，绝不复活或发布；数据错误持久化 `stopped=failed`（不可恢复）；
暂时的磁盘 OSError 持久化 `stopped=io_error`（可恢复），故障解除后从
最后已确认边界接续；崩溃不留 stopped 标记，因此天然可恢复。收束时先
`writer.join()` 等在途 I/O，再写终态，不留能改状态的后台线程。run 内
容/长度或持久状态损坏 → 明确拒绝（`StateCorrupt`/`ResumeRefused`），
不删除证据、不默默重算。恢复时校验全部确认 run 的 sha256（允许——这
不是重读原始输入）；输入被替换（即使字节一样）、追加、截断或元数据变
化，或要求更改 dedup/单行上限 → 在写任何任务数据、删除 run 或碰输出
之前拒绝。恢复可用不同预算与块/扇入，只要 MWS ≤ 新预算；不合规配置在
消耗或破坏任何进度之前拒绝。

**保留与回收**：DONE/CANCELLED 自动删除 runs/ 与暂存输出（状态目录只
剩 LOCK/task.json/progress.json/events.jsonl 这份持久记录）；FAILED 保
留 runs 供诊断；ERROR（可恢复）保留全部以待接续。不再需要任务时，整
个 state_dir 可由运维手动删除——清理只发生在 state_dir 内部与
`<output>.tmp.<task_id>*`，永远不会碰输入或别的任务。

## 内存与可观察性

run 读写走与普通排序相同的预留缓冲池/写线程（buffer/io 类），归并读
用 `RunReader`（merge-read/merge-key），状态编码与校验和核验使用一个
64KB 的常驻 `state` 类预留（MWS 因此为普通排序公式 + 2×64KB）；检查点
只移动文件名与偏移，绝不把整份输入或所有 run 装进内存。内存中的
reservation 不持久化，下一进程从磁盘状态重建。`status()` 暴露：检查点
代次、confirmed_offset/next_lineno、确认 run 数、本次尝试真实来源读
取字节与归并工作量、复用 run 数、attempt/recoveries、终态；事件日志
（`RSORT_*`）给出每次尝试的来源读取范围、实际执行的归并组、检查点、
发布与孤儿清理，统计不掩盖重做的工作。任务结束后本次额度归零（所有
测试断言 `total_reserved == 0`、`DOUBLE_RELEASE == 0`）。

## 测试（`tests/test_resumable.py`、`tests/test_resume_kill.py`）

进程内：字节级参考对照（sort/dedup）、无换行末行、空输入、检查点节奏、
可恢复 I/O 错误换预算恢复、持久取消、shutdown 挂起后可恢复、run 截断/
改字节拒绝、progress 损坏拒绝、输入替换/追加/截断/touch 拒绝、语义变更
拒绝、双 runner 锁互斥、与普通排序共享仲裁器、DONE 幂等/目标被动、发
布前冲突、后段坏行、超长行。

独立子进程（真实 SIGKILL 闸门）：run 写好未确认、归并已确认未回收、目
标已替换 DONE 未记录三个精确闸门；输入处理一部分后杀掉换预算恢复；多
轮恢复穿过多趟归并（热键跨检查点，与未中断对照运行逐字节一致、确认组
序列一致、重做量 ≤ 崩溃次数）；真实第二进程互斥与杀死后接管；发布窗
口外部替换目标；真实 I/O 失败后恢复；取消后重开；大样本运行时生成、
流式校验、按尝试独立记录来源读取范围与归并组执行（证据落盘
`artifacts/rsort-kill-*/`）。

---

# 外存分区作业（repartition）

`extsort.repartition.RepartitionJob` 在同一个 `BufferArbiter` 之上把未排序
JSONL **逐条路由**到固定分片：不排序、不去重、不聚合，同一分片内严格保持
记录的输入先后，原始记录字节（含已有行结束符）逐字节保留；仅当输入末行
没有换行时补一个 LF，避免与后续记录粘连。提交入口与其它作业共用一个
runner：

```python
jid = runner.submit_repartition("in.jsonl", "/srv/dataset",
                                key_pointer="/user/id", num_partitions=64,
                                max_open_shards=8)
runner.status(jid)      # 同一套 status/cancel/wait
runner.cancel(jid); runner.wait(jid)
```

CLI：

```bash
python -m extsort repartition --input in.jsonl --outdir /srv/dataset \
    --key-pointer /user/id --partitions 64 --max-open-shards 8 \
    --budget 64MB --workdir ./work
```

## 输入合同

每行必须是一个 JSON 对象（UTF-8 JSONL）。调用者给一个 RFC 6901 JSON
Pointer，它必须解析到对象属性或数组有效下标，最终值必须是字符串（允许
空字符串）。缺失路径、类型不符（解析结果不是 string）、空行、非法 JSON、
非标准数值常量（NaN/Infinity/-Infinity）、超过 `max_line_bytes` 的记录都
让整项作业失败，错误给出从 1 开始的**物理行号与原因**（`RepartitionInputError`），
绝不跳过坏行或发布半成品。分区数是 1..4096 的整数。

## 固定映射（与运行时状态无关）

```
shard = uint64_be( sha256( key 的 UTF-8 字节 ).digest()[:8] ) mod N
```

相同键永远落在同一分片；映射只依赖键字节与 N，不依赖 Python 进程哈希
随机化或任何运行时状态，跨进程重复运行结果一致。

## 数据集目录、清单与原子发布

每次执行写一个全新的唯一目录，旧 generation 永不删除：

```
<outdir>/
  CURRENT                         # UTF-8 小文件，内容是当前完整 generation_id
  generations/<generation_id>/
    manifest.json
    part-00000.jsonl ... part-(N-1).jsonl   # 空分片也是零字节文件
```

`manifest.json` 记录格式版本、键指针、分区算法（sha256 / 前 8 字节 /
无符号大端 / 取模及模数）、分区数、总记录数、输入 SHA-256，以及每个分片的
记录数、字节数和 SHA-256。先把所有分片与清单完整落盘并关闭，再用
**同目录临时文件 + `os.replace` 原子替换 CURRENT**——这唯一的替换就是提交
边界：边界之前的任何校验失败、取消或磁盘错误都不改变已有 CURRENT 及其
指向的数据，只清理本作业新建的临时 generation（含 runner 工作目录里的
spill 暂存）；边界之后到达的取消仍报告成功。读取方只通过 CURRENT 定位
当前完整数据集。

## 有界文件描述符与缓冲（不随分区数膨胀）

调用者用 `max_open_shards`（1..N）限制同时打开的分片数据文件数，实际
打开数绝不超过它。实现是有界 LRU 分片缓存 + 有序临时片段：

- 同时打开的分片文件与驻留分片写缓冲都不超过 `max_open_shards`；缓存满
  时把最久未用分片的脏缓冲刷到一个**共享追加型 spill 文件**（记录该分片
  的 `(offset, length)` 时间序片段）并关闭文件描述符，缓冲进入有界空闲池
  循环复用，热路径没有 rename，也不为每个分片永久保留 fd 或无界缓冲。
- 路由结束后，每个分片按其时间序片段从 spill 文件顺序合并出最终 part，
  因此分片被反复开关也不改变记录顺序；合并读块同样先预留后读取。
- 所有驻留字节（输入块 `line`、键 `key`、分片写缓冲 `buffer`、同步在途
  I/O `io`、合并读 `merge-read`）都走同一个 `BufferArbiter` 的 B，复制前
  先预留；阻塞式预留只在路由锁之外进行，协作回收回调在锁内只做
  flush/释放、绝不调用 reserve，因此与等待额度的工作线程之间无死锁。
  MWS 在提交时计算（含 `max_open_shards` 个写缓冲槽），装不下最小工作集
  的配置在启动时即以 `ConfigError` 拒绝。

## 阶段与可观察性

`status().phase` 依次为 `read -> route -> merge -> manifest -> publish`，
并暴露输入行数、已路由记录数、输入/分片写字节、分片打开高水位、驱逐次数、
冲刷次数、片段合并次数、当前/峰值预留、MWS、generation 与提交标志；
`report()` 给出各 kind 峰值、预算高水位、VmHWM 真实峰值 RSS 与 Python
元数据估算。与排序、时间范围关联、可恢复排序、分位作业共用运行器时，各
作业独立准入、互不污染，失败只清理本作业并归还全部预留。

## 测试

| 测试 | 内容 |
|------|------|
| `test_repartition_correctness.py` | 独立指针解析与 SHA-256 路由参考（`rp_ref.py`，不 import 产品代码）：嵌套对象/数组路径、Pointer 转义、空键、相同键、空分片、无换行末行、多种缓冲/打开数几何逐字节一致；同输入重复运行及跨 `python -I` 子进程的分片归属一致；N=1 时输出与输入逐字节相同 |
| `test_repartition_lifecycle.py` | 全部合同违约（物理行号+原因）、超长行、提交期配置/MWS 拒绝；路由中/提交边界前后的取消语义；分片写失败、清单失败、后段坏行均与预置旧 CURRENT/旧分片逐字节比较；外线程回收回调无死锁且真释放额度；阶段与计数可观察、预算归零；兄弟作业失败隔离 |
| `test_repartition_scale.py` | 运行时造数，记录交错分布到大量分区；真实降低 `RLIMIT_NOFILE` 且 `max_open_shards=3` 下无漏行/重复/乱序，逐事件核对共享预留 ≤ B；紧预算协作回收、与普通排序并发完成且额度归零；多种小缓冲几何输出一致。大数据与暂存只在运行时生成于临时目录，不提交 Git |



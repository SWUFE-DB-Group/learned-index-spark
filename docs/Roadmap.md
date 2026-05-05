Chốt luôn: bản reproduce chính của bạn là LiLIS-K trên Spark RDD, với local index = Y-axis sort + taut-string spline + ε=32 + radix bits=10. Không dùng Z-order cho main path, không dùng regression/segment-based model, không bắt đầu bằng DataFrame rewrite, và không lấy NYC full làm target vòng đầu trên cluster 2×16GB. Tôi bám vào paper LiLIS  và bản deep analysis repo bạn dán , cùng code repo tác giả: Spline.build() sort theo Y và gọi tautString, BuildIndex build bằng mapPartitions, còn benchmark build-cost đi qua BuildAll.
Source of truth
Quy tắc ra quyết định của team 2 người là:
Paper quyết định semantic cần reproduce: local learned spline có error bound, spatial-aware partitioning, point/range/kNN/join semantics, Spark standalone, workload protocol.
Repo quyết định chi tiết implementation khi paper để mở: sort key thực tế, build path local index, radix cho floating key, build driver, query code path.
Nếu repo lệch paper ở phần core semantics, paper thắng. Có 2 chỗ lệch rõ nhất:
sampling: paper nói uniform 1%, repo hiện tính fraction qua utility của Sedona chứ không hardcode 1%;
join: paper mô tả broadcast polygon + range lookup + refine, repo lại gọi linearScanLookUp.
Chủ đề
Paper
Repo tác giả
Final choice
Runtime
Java + Spark standalone; paper evaluate trên Spark 3.0. 
README yêu cầu Spark 3.0+, HDFS, Java 8+, Maven. 
Pin Spark 3.0.x cho baseline reproduce. Chỉ mở nhánh compatibility nếu cluster của bạn buộc dùng 3.5.x.
Core API
Kiến trúc bám Spark RDD, local index build trong partition. 
BuildIndex.indexBuild() = pointJavaRDD.mapPartitions(new IndexBuilder()). 
RDD là core path. DataFrame chỉ dùng cho baseline Spark/Sedona.
Sort key local index
Paper cho phép key là 1 trục hoặc aggregated value. 
Spline.build() gọi Utils.sortPointsY(points). 
Dùng Y-axis. Không đổi sang Z-order trong bản reproduce chính.
Local model
Error-bounded spline interpolation. 
SplineUtil.tautString(cdf, SPLINE_SIZE). 
Taut-string only. Xóa mọi nhánh regression / segment-based model.
Hyperparams
Default error bound = 32, spline bits = 10. 
SPLINE_SIZE = 32, RADIX_SIZE = 10. 
Giữ nguyên ε=32, b=10 ở pass đầu. Không tune.
Floating-key radix
Paper có radix table cho floating key. 
transform(val) cast từ (val - min_y) * factor, radixHint size = 2^b + 2. 
Giữ nguyên design repo.
Build stage
Local build phải nhẹ, không thêm shuffle. 
BuildIndex chỉ mapPartitions; shuffle nằm ở repartition stage. 
Một shuffle duy nhất ở partitioning; build local tuyệt đối không shuffle.
Default partitioner
Paper có nhiều partitioner; LiLIS-K là default implementation. 
Repo có KD/Quad/R-tree/Grid; BuildAll chạy 5 variant. 
KD-tree là default reproduce path.
Sampling
Paper nói uniform 1%. 
KDB/R-tree tính sampleNumberOfRecords rồi fraction, không hardcode 1%. 
Trong reimplementation của bạn: default sample_rate=0.01. Giữ thêm compat_repo_sampling=true nếu muốn so repo byte-for-byte.
R-tree overflow
Paper nói cần overflow grid cho object không thuộc leaf R-tree. 
RtreePatitioner không có overflow bucket tường minh; unmatched object bị trả về partition 0. 
Không dùng R-tree làm main path. Nếu làm variant R-tree, phải thêm overflow bucket riêng.
Point/Range query
Two-phase filtering + local learned search. 
PointQuery/RangeQuery filter theo bbox rồi mapPartitions; RangeFilterUsingIndex gọi spline.lookUp. 
Giữ đúng pattern paper+repo.
kNN
Repeated range query theo Eq. (1)(2)(3). 
KNNQuery tính d=N/area, r=sqrt(k/(πd)), lặp RangeQuery. 
Giữ nguyên logic này.
Join
Paper: polygon broadcast + MBR range query + polygon refine. 
JoinQueryUsingIndex comment out lookUp, gọi linearScanLookUp. 
Join không nằm trên critical path 7 ngày. Nếu làm, phải rewrite theo paper; không copy repo join hiện tại.
Build benchmark driver
Paper có figure build-cost across variants. 
BuildAll gọi QP/KP/RP/FP/APIndexBuild. 
Dùng BuildAll cho build-cost reproduction.
CSV/HDFS
  -> JavaRDD<Point(x=longitude, y=latitude)>
  -> sample 1% uniformly
  -> build KD-tree leaf grids
  -> map Point -> (leafId, Point)
  -> repartitionByKey                       // shuffle x1
  -> JavaRDD<Point> partitioned
  -> mapPartitions(IndexBuilder)
       bounds
       if n <= 100: linear_scan = true
       else:
         sort by Y
         CDF-on-the-fly on Y
         taut-string spline (epsilon = 32)
         radix table (10 bits)
  -> JavaRDD<Spline>
  -> persist(MEMORY_AND_DISK_SER)
  -> point / range / kNN
  -> optional join (broadcast polygons -> MBR range lookup -> refine)
​
Điểm phải giữ nguyên:
LiLIS core dùng RDD, vì repo build index bằng mapPartitions trên JavaRDD<Point> và point/range query đều chạy trên JavaRDD<Spline>, không phải DataFrame path.
Một shuffle duy nhất nằm ở partitioning, vì KDBTreePartitioner sinh (leafId, point) rồi partitionBy(...), sau đó mới map về Point; build index không thêm shuffle.
Local search hoàn toàn bám Y-bound, vì Spline.lookUp() reject/accept dựa trên min_y/max_y, estimate interval theo Y rồi refine bằng X.
Step-by-step Roadmap
PHASE 0 — ALIGNMENT
Mục tiêu: khóa design để team không trôi sang một hệ khác.
Việc phải làm ngay
Tạo docs/repro_spec.md và ghi cứng các choice sau:
sort_key = Y
local_model = taut_string_spline
epsilon = 32
radix_bits = 10
partitioner_default = KD_TREE
sample_rate_default = 0.01
fallback_linear_scan_threshold = 100
runtime_baseline = Spark Standalone 3.0.x
Xóa hoặc disable mọi nhánh code đang thử:
Z-order local key
segment-based model
LinearRegression / sklearn path
DataFrame-first rewrite
Tách branch:
core-repro cho LiLIS
baseline-sedona cho Sedona/Spark baseline
Định nghĩa output chuẩn:
build time
mean latency / 50 runs
median & p95 nội bộ
scan size
result cardinality
Exit criteria
Cả team trả lời giống nhau cho 5 câu: key là gì, model là gì, ε/b là gì, partitioner default là gì, join có nằm critical path không.
PHASE 1 — LOCAL REBUILD (paper-faithful)
Mục tiêu: local learned index đúng paper/repo trước khi động vào cluster.
Repo hiện tại build local index trong spline/Spline.java bằng Y-sort, tautString, radix hints, và fallback n<=100 sang linear scan.
Việc cần làm
Đóng băng local build path
File: src/main/java/spline/Spline.java
Bảo đảm build() chỉ còn đúng flow:
compute bounds
fallback if size<=100
Utils.sortPointsY(points)
new CdfOnTheFlyInterfaceY(points)
SplineUtil.tautString(...)
buildRadix(10)
Không cho cắm model khác.
Patch bug-risk ở spline util
File: src/main/java/utils/SplineUtil.java
tautString() đang append phần tử cuối trong một trạng thái dễ lỗi; patch theo hướng an toàn, ví dụ chỉ append khi current còn valid.
interpolate() có biểu thức next.getX() - prev.getY() rất đáng ngờ; đây không nằm trên critical path query, nên không chặn Phase 1, nhưng phải comment rõ và sửa nếu bạn dùng computeErrors() trong test harness.
Viết test parity local
SplineBuildTest
PointQueryParityTest
RangeQueryParityTest
KNNParityTest
Exact oracle = brute-force linear scan trên cùng tập điểm.
Dữ liệu local để verify
CHI subset 100k, 1M
SYN subset 1M, 5M
Luôn có case duplicate Y, duplicate point, skewed Y distribution.
Instrumentation
expose:
isLinearScan
số spline points
estimated-from / estimated-to
scanned points count
Pass criteria
100/100 point query random = exact match brute force.
100/100 range query random = exact match brute force.
100/100 kNN random = exact same top-k as brute force.
Không exception ở tautString.
Không có code path regression/segment model còn sống.
PHASE 2 — PARTITIONING
Mục tiêu: global partitioning đúng semantic paper, nhưng practical cho cluster 2×16GB.
Repo KDB partitioner sample rồi build tree, sau đó partitionBy(...); còn analyze() chỉ tính boundary/count, không encode cứng sampling 1%.
Quyết định
Bỏ Z-quantile nếu team đang có.
Implement KD-tree trước, vì đây là default reproduce path.
Sampling mặc định = 1% explicit trong code của bạn.
R-tree không phải main path ở vòng đầu.
Việc cần làm
File: src/main/java/partitions/KDBTreePartitioner.java
thêm sampleRate config
mặc định 0.01
log:
actual sample count
fraction
leaf count
points/partition histogram
Giữ đúng flow:
sample(false, fraction)
build KD-tree leaf zones
map point -> (leafId, point)
partitionBy
bỏ key
không build index trong partitioner
Viết PartitionDiagnostics.java
total count before/after repartition
min/avg/max points per partition
number of partitions with n<=100
bbox area per partition
R-tree variant chỉ làm sau khi K stable
File: src/main/java/partitions/RtreePatitioner.java
thêm explicit overflow bucket
không dùng “partition 0” như fallback cho unmatched object. Repo hiện làm vậy, không đủ paper-faithful.
Partition sizing cho máy của bạn
CHI full (7M): bắt đầu với 16 partitions
SYN 10M: 16 partitions
SYN 30M: 32 partitions
SYN 50M: 48 partitions
SYN 100M: chỉ thử khi mọi thứ ổn định, dùng 64 partitions
Pass criteria
count(before) == count(after)
Không có partition cực lệch kéo một executor chết
Tỷ lệ partition rơi vào linear_scan=true trên CHI không quá cao; nếu >15%, giảm số partitions
PHASE 3 — QUERY ENGINE
Mục tiêu: point/range/kNN đúng paper+repo; join để sau.
Point/range path của repo đang đúng pattern two-phase filtering: bbox prune trước, local lookup sau; kNN cũng đang theo repeated range-query. Join là điểm lệch rõ nhất vì repo gọi linearScanLookUp.
3.1 Point Query
Files
src/main/java/query/PointQuery.java
src/main/java/query/PointsFilterUsingIndex.java
Việc cần làm
Global filter bằng partition bbox.
Local search:
estimate position theo learned index
binary search trong p̂ ± ε
scan hai phía trên đoạn cùng key/Y
check exact (x,y)
Verify
50 query hit + 50 query miss trên CHI subset
parity tuyệt đối với brute force
3.2 Range Query
Files
src/main/java/query/RangeQuery.java
src/main/java/query/RangeFilterUsingIndex.java
src/main/java/spline/Spline.java
Việc cần làm
Giữ two-phase:
global bbox prune
local spline.lookUp(rect, result)
Giữ fast-path:
completely outside -> return empty
partition fully inside window -> return all points
small partition -> linear scan
Thêm QueryStats:
partitions visited
scanned points
returned points
Verify
100 random rectangles
exact parity với brute force
scan-size giảm rõ ràng so với full scan
3.3 kNN
File
src/main/java/query/KNNQuery.java
Việc cần làm
Giữ đúng công thức:
d = N / area
r = sqrt(k / (πd))
lặp range query
nếu res=0 thì expand
nếu res>0 && res<k thì recompute density local
cuối cùng exact top-k theo distance
Cache N, area, global bbox trong metadata, không recompute mỗi query
Verify
k in {1, 10, 100}
exact top-k như brute force
3.4 Join (optional, not critical path)
File
src/main/java/query/JoinQueryUsingIndex.java
Việc cần làm nếu thực sự cần
broadcast polygon list
polygon -> MBR
spline.lookUp(queryRange, rangePoints) chứ không linearScanLookUp
refine bằng polygon.contains(point)
Quyết định
Không đưa join vào deadline 7 ngày trừ khi point/range/kNN đã ổn định.
Không claim reproduce join nếu vẫn dùng repo join hiện tại.
PHASE 4 — SPARK MIGRATION
Mục tiêu: đưa pipeline lên cluster 2 máy, vẫn bám kiến trúc paper.
Paper evaluate trên Spark standalone với 7 máy × 8GB; repo README cũng bám Spark 3.0+, HDFS. Máy của bạn có tổng RAM ít hơn paper, nên CHI full là main target; SYN phải scale dần; NYC full không phải target vòng đầu.
Cấu hình Spark khuyến nghị
mode: Spark Standalone
workers: 2
executors: 1 executor / machine
-executor-memory 8g
-executor-cores 4
-driver-memory 4g
-conf spark.default.parallelism=16
-conf spark.sql.shuffle.partitions=16
-conf spark.serializer=org.apache.spark.serializer.KryoSerializer
-conf spark.rdd.compress=true
Việc cần làm
Giữ LiLIS core ở RDD
JavaRDD<Point>
JavaRDD<Spline>
mapPartitions
repartition/partitionBy
Persist đúng chỗ
splineRDD.persist(MEMORY_AND_DISK_SER)
materialize bằng count()
sau đó rawPointRDD.unpersist()
Tách baseline khỏi core
benchmark/QueryBenchmark.java
benchmark/SparkNoIndexBaseline.java
benchmark/SedonaBaseline.java
Build-cost path
dùng idnexbuild/BuildAll.java
Không rewrite sang DataFrame trước
DataFrame chỉ dùng cho baseline Spark scan nếu tiện hơn trong stack của bạn
Pass criteria
CHI full build xong trên 2 máy không OOM
CHI point/range/kNN chạy ổn 50 lần
Spark UI cho thấy build stage không có shuffle ngoài partitioning
PHASE 5 — EVALUATION
Mục tiêu: lấy được bộ số liệu đủ để nói “đã reproduce core claims trên cluster nhỏ hơn”.
Dataset plan cho 2×16GB
Dataset
Quyết định
Lý do
CHI full
MUST
1.9GB / 7M là anchor dataset thực tế nhỏ nhất trong paper/repo.  
SYN
MUST, nhưng scale dần 10M -> 30M -> 50M
Dùng để verify behavior trên dữ liệu uniform mà không tự sát bộ nhớ.
SYN 100M
OPTIONAL stretch
Paper có 100M, nhưng cluster của bạn nhỏ hơn đáng kể. 
NYC full
OPTIONAL / skip
20GB / 300M không hợp target vòng đầu trên 32GB tổng RAM.  
Benchmark matrix
Bắt buộc
LiLIS-K
Spark no-index
Sedona-N
Nên có
LiLIS-Q
LiLIS-F / LiLIS-A
Sedona-RK / Sedona-RQ
Để sau
LiLIS-R
join benchmark
Metrics
build time
mean query latency over 50 runs
median / p95 nội bộ
scan size
partitions scanned
points scanned locally
result size
Measurement protocol
warm-up 1 lần
sau đó 50 runs
official number = mean để gần paper nhất
internal sanity = median + p95
timer chỉ dừng sau action (count() / collect()), không dừng ở transformation creation
Success criteria thực tế
correctness parity = 100%
build-cost có đủ 5 số LiLIS variant
LiLIS-K thắng Spark no-index và Sedona-N trên CHI ở point/range/kNN
thứ tự chất lượng partitioner nhìn chung gần paper:
tree-based tốt hơn grid-based
KD-tree là default an toàn
exact absolute numbers có thể lệch paper, vì cluster của bạn nhỏ hơn
Insight cần operationalize
Việc cần làm ngay
File/module
Risk nếu làm sai
Verify
Local key thực tế là Y
Khóa sortPointsY làm main path
spline/Spline.java
Reproduce thành hệ khác (Z-order variant)
grep code path + unit test sorted-by-Y
Model phải là taut-string
Xóa alt model; patch tautString an toàn
utils/SplineUtil.java
Kết quả không còn paper-faithful
spline parity vs brute force
ε=32, b=10 là default reproduce
In config ra log startup
spline/Spline.java, config/ReproConfig.java
So sánh số liệu vô nghĩa
startup manifest
Build local index không được shuffle
Giữ BuildIndex chỉ mapPartitions
index/BuildIndex.java
Build time phình to
Spark UI stage graph
KD-tree partitioner là default
Thêm sampleRate=0.01 + diagnostics
partitions/KDBTreePartitioner.java
Partition quality trôi, fallback quá nhiều
histogram + fallback ratio
R-tree repo đang không paper-faithful
Không dùng làm main path; nếu cần thì thêm overflow bucket
partitions/RtreePatitioner.java
Claim sai về R-tree variant
unmatched objects count = 0 ngoài overflow
Point/range là two-phase filter + local lookup
Giữ bbox prune + mapPartitions + lookUp
query/PointQuery.java, query/RangeQuery.java, query/RangeFilterUsingIndex.java
Scan toàn cluster
partitions scanned/query
kNN = repeated range query
Giữ công thức paper và exact top-k refine
query/KNNQuery.java
Sai kết quả hoặc latency bất thường
parity với brute force
Join repo đang lệch paper
Hoãn hoặc rewrite sang lookUp + broadcast + refine
query/JoinQueryUsingIndex.java
Claim join reproduce sai
code review: không còn linearScanLookUp
Build benchmark phải dùng đúng driver
Dùng BuildAll + *IndexBuild, không ad-hoc main
idnexbuild/BuildAll.java, idnexbuild/*
Số build-cost sai
đủ 5 variants trong output
Cần baseline thật
Viết benchmark runner riêng
benchmark/QueryBenchmark.java, benchmark/SedonaBaseline.java
Không đối chiếu được paper
bảng latency hoàn chỉnh
Ưu tiên công việc
MUST DO
Chốt Y-sort + taut-string + ε=32 + b=10
Giữ RDD + mapPartitions làm core
Dùng KD-tree làm default path
Viết parity tests cho point/range/kNN
Dùng BuildAll cho build-cost
Chạy CHI full
Có baseline Spark no-index và Sedona-N
SHOULD DO
Explicit sample_rate=0.01
QueryStats / scan-size instrumentation
SYN 10M/30M/50M
LiLIS-Q/F/A ablation
Sedona-RK/RQ nếu dependency ổn
OPTIONAL
Join rewrite đúng paper
R-tree overflow bucket
Spark 3.5 compatibility pass
SYN 100M stretch run
SAI HƯỚNG
Đổi main path sang Z-order
Giữ segment-based model hoặc LinearRegression
Rewrite DataFrame-first
Dùng repo join hiện tại rồi nói là “paper-faithful join”
Chạy NYC full ngay vòng đầu
Tune ε/b trước khi có baseline
Đo thời gian trước action
Risk & Failure Points
Algorithm drift
Triệu chứng: team A build Z-order, team B benchmark Y-sort.
Chặn bằng repro_spec.md + startup config manifest.
tautString() lỗi tail-state
Triệu chứng: exception ở partition nhỏ/lạ.
Chặn bằng patch sớm + unit test edge cases.
interpolate() bug làm hỏng error-analysis
Không block query path, nhưng block nếu bạn dùng computeErrors() để assert ε.
Chặn bằng comment rõ hoặc sửa luôn ở test branch.
Sampling drift
Paper 1%, repo dynamic fraction.
Chặn bằng explicit sample_rate=0.01 và log actual sample count.
Too many tiny partitions
Triệu chứng: learned index lợi ít, linear-scan fallback quá nhiều.
Chặn bằng diagnostics + giảm partition count.
Join false positive về “đã reproduce”
Repo join đang linear scan trong MBR.
Chặn bằng cách hoặc rewrite đúng paper, hoặc tuyên bố join chưa included.
Timing sai vì Spark lazy
Chặn bằng count() / collect() mới stop timer.
OOM trên SYN/NYC
Chặn bằng staged dataset plan, serialized persistence, unpersist raw RDD sau khi index build xong.
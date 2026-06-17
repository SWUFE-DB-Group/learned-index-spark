package idnexbuild;

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import partitions.SpatialPartition;
import pointrdd.PointRDDUtils;
import query.KNNQuery;
import query.PointQuery;
import query.RangeQuery;
import spline.Spline;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * QueryBenchmarkComparison — Phase 3 (50 random query points).
 *
 * Cải tiến so với version cũ:
 *   - Dùng takeSample() để lấy 50 điểm ngẫu nhiên từ dataset
 *     thay vì lặp 50 lần trên 1 điểm cố định
 *   - Mỗi run i dùng queryPoints[i] → tránh JVM cache warming
 *   - Đúng với paper: "50 independent runs"
 *
 * Memory strategy (11GB RAM) — sequential, không contention:
 *
 *   Bước 0: Load pointRDD → takeSample(50 points) → lưu vào List
 *   Hiệp 1: Build splineRDD (SER) → query LiLIS 50 runs → unpersist splineRDD
 *   Hiệp 2: Persist pointRDD (SER) → query No-index 10 runs → unpersist pointRDD
 *
 * KEY: takeSample() chạy 1 lần duy nhất ở Bước 0, trước khi build index.
 * Không cần persist pointRDD để lấy sample — chỉ trigger 1 lần đọc CSV.
 */
public class QueryBenchmarkComparison {

    static final int    LILIS_RUNS   = 50;
    static final int    NO_IDX_RUNS  = 10;
    static final int    WARMUP       = 2;
    static final int    KNN_K        = 10;
    static final long   SAMPLE_SEED  = 42L;

    // Range query window: cạnh ~0.013 độ (~1.4km) xung quanh mỗi query point
    // Tương ứng selectivity ~0.00001% theo paper Section V.A.3
    static final double RANGE_HALF   = 0.0065;

    // NYC stats cố định — từ results/figures/nyc_xy_stats.txt
    static final double TOTAL_AREA   = (-72.1965 - (-74.9996)) * (41.9237 - 40.0081);
    static final long   TOTAL_N      = 46_475_157L;

    public static void main(String[] args) throws Exception {

        String datasetLabel = args[0];
        String dataPath     = args[1];
        String outputFile   = "results/" + datasetLabel + "_query_comparison.txt";

        SparkConf conf = new SparkConf().setAppName("SparkApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ══════════════════════════════════════════════════════════
        // BƯỚC 0: Load pointRDD và lấy 50 query points ngẫu nhiên
        // takeSample() trigger đọc CSV 1 lần → lưu vào List Java
        // Không persist pointRDD ở đây để tiết kiệm RAM cho Hiệp 1
        // ══════════════════════════════════════════════════════════
        System.out.println("[Bước 0] Loading data và sampling 50 query points...");
        JavaRDD<Point> pointRDD = PointRDDUtils.CreatePointRDD(sc, dataPath, 0);

        // takeSample(withReplacement=false, num=50, seed=42)
        List<Point> queryPoints = pointRDD.takeSample(false, LILIS_RUNS, SAMPLE_SEED);
        System.out.println("[Bước 0] Đã lấy " + queryPoints.size() + " query points ngẫu nhiên.");

        // Tạo range rectangles tương ứng cho mỗi query point
        Rectangle[] queryRanges = new Rectangle[queryPoints.size()];
        for (int i = 0; i < queryPoints.size(); i++) {
            double cx = queryPoints.get(i).getX();
            double cy = queryPoints.get(i).getY();
            queryRanges[i] = new Rectangle(
                    new Point(cx - RANGE_HALF, cy - RANGE_HALF),
                    new Point(cx + RANGE_HALF, cy + RANGE_HALF)
            );
        }

        // ══════════════════════════════════════════════════════════
        // HIỆP 1 — SÂN KHẤU LiLIS-K (chỉ splineRDD trong RAM)
        // ══════════════════════════════════════════════════════════
        System.out.println("\n" + "=".repeat(60));
        System.out.println("HIỆP 1 — Benchmark LiLIS-K (" + LILIS_RUNS + " runs)");
        System.out.println("=".repeat(60));

        // Build index — pointRDD không cache, Spark đọc lại CSV 1 lần
        System.out.println("[Hiệp 1] Building LiLIS-K index...");
        JavaRDD<Point> partitionRDD = SpatialPartition.KDBTreePartitioner(pointRDD);
        JavaRDD<Spline> splineRDD   = BuildIndex.indexBuild(partitionRDD);
        splineRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        // Trigger build — splineRDD vào RAM, pointRDD bị GC tự nhiên
        long splineCount = splineRDD.count();
        System.out.println("[Hiệp 1] Index CACHED (SER): " + splineCount + " partitions");

        long[] pointLiLIS = new long[LILIS_RUNS];
        long[] rangeLiLIS = new long[LILIS_RUNS];
        long[] knnLiLIS   = new long[LILIS_RUNS];

        // Warm-up với 2 điểm đầu
        System.out.println("[Hiệp 1] Warm-up " + WARMUP + " runs...");
        for (int w = 0; w < WARMUP; w++) {
            Point  wp = queryPoints.get(w);
            Rectangle wr = queryRanges[w];
            PointQuery.SpatialPointQuery(splineRDD, wp).count();
            RangeQuery.SpatialRangeQuery(splineRDD, wr).count();
            KNNQuery.SpatialKNNQuery(splineRDD, KNN_K, wp, TOTAL_AREA, TOTAL_N);
        }

        // 50 timed runs — mỗi run dùng queryPoints[i] khác nhau
        System.out.println("[Hiệp 1] Running " + LILIS_RUNS + " timed runs...");
        for (int i = 0; i < LILIS_RUNS; i++) {
            Point     qp = queryPoints.get(i);
            Rectangle qr = queryRanges[i];
            long t0, t1;

            t0 = System.currentTimeMillis();
            PointQuery.SpatialPointQuery(splineRDD, qp).count();
            t1 = System.currentTimeMillis();
            pointLiLIS[i] = t1 - t0;

            t0 = System.currentTimeMillis();
            RangeQuery.SpatialRangeQuery(splineRDD, qr).count();
            t1 = System.currentTimeMillis();
            rangeLiLIS[i] = t1 - t0;

            t0 = System.currentTimeMillis();
            KNNQuery.SpatialKNNQuery(splineRDD, KNN_K, qp, TOTAL_AREA, TOTAL_N);
            t1 = System.currentTimeMillis();
            knnLiLIS[i] = t1 - t0;

            if ((i + 1) % 10 == 0) {
                System.out.printf("[LiLIS] Run %d/%d | point=%dms range=%dms knn=%dms%n",
                        i + 1, LILIS_RUNS, pointLiLIS[i], rangeLiLIS[i], knnLiLIS[i]);
            }
        }

        // Dọn sân — xả splineRDD trước Hiệp 2
        splineRDD.unpersist(true);
        System.out.println("[Hiệp 1] DONE. splineRDD released.");

        // ══════════════════════════════════════════════════════════
        // HIỆP 2 — SÂN KHẤU NO-INDEX (chỉ pointRDD trong RAM)
        // ══════════════════════════════════════════════════════════
        System.out.println("\n" + "=".repeat(60));
        System.out.println("HIỆP 2 — Benchmark No-Index (" + NO_IDX_RUNS + " runs)");
        System.out.println("=".repeat(60));

        // Bây giờ RAM sạch → mới cache pointRDD
        pointRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
        long rawCount = pointRDD.count();
        System.out.println("[Hiệp 2] pointRDD CACHED (SER): " + rawCount + " points");

        long[] pointNoIdx = new long[NO_IDX_RUNS];
        long[] rangeNoIdx = new long[NO_IDX_RUNS];
        long[] knnNoIdx   = new long[NO_IDX_RUNS];

        // Filter function dùng anonymous class để tránh serialization issues
        // Warm-up
        System.out.println("[Hiệp 2] Warm-up " + WARMUP + " runs...");
        for (int w = 0; w < WARMUP; w++) {
            final double wx = queryPoints.get(w).getX();
            final double wy = queryPoints.get(w).getY();
            Rectangle wr = queryRanges[w];
            pointRDD.filter(new Function<Point, Boolean>() {
                public Boolean call(Point p) { return p.getX() == wx && p.getY() == wy; }
            }).count();
            RangeQuery.SpatialRangeQueryWithOutIndex(pointRDD, wr).count();
            KNNQuery.SpatialKNNQuerywithoutIndex(pointRDD, KNN_K,
                    queryPoints.get(w), TOTAL_AREA, TOTAL_N);
        }

        // 10 timed runs — dùng 10 điểm đầu trong queryPoints
        System.out.println("[Hiệp 2] Running " + NO_IDX_RUNS + " timed runs...");
        for (int i = 0; i < NO_IDX_RUNS; i++) {
            final double qx = queryPoints.get(i).getX();
            final double qy = queryPoints.get(i).getY();
            Rectangle qr = queryRanges[i];
            Point qp = queryPoints.get(i);
            long t0, t1;

            t0 = System.currentTimeMillis();
            pointRDD.filter(new Function<Point, Boolean>() {
                public Boolean call(Point p) { return p.getX() == qx && p.getY() == qy; }
            }).count();
            t1 = System.currentTimeMillis();
            pointNoIdx[i] = t1 - t0;

            t0 = System.currentTimeMillis();
            RangeQuery.SpatialRangeQueryWithOutIndex(pointRDD, qr).count();
            t1 = System.currentTimeMillis();
            rangeNoIdx[i] = t1 - t0;

            t0 = System.currentTimeMillis();
            KNNQuery.SpatialKNNQuerywithoutIndex(pointRDD, KNN_K, qp, TOTAL_AREA, TOTAL_N);
            t1 = System.currentTimeMillis();
            knnNoIdx[i] = t1 - t0;

            System.out.printf("[NoIdx] Run %d/%d | point=%dms range=%dms knn=%dms%n",
                    i + 1, NO_IDX_RUNS, pointNoIdx[i], rangeNoIdx[i], knnNoIdx[i]);
        }

        pointRDD.unpersist(true);
        sc.close();

        writeResults(outputFile, datasetLabel,
                pointLiLIS, rangeLiLIS, knnLiLIS,
                pointNoIdx, rangeNoIdx, knnNoIdx);
    }

    // ── Helpers ──
    static double avg(long[] a) {
        long s = 0; for (long v : a) s += v; return (double) s / a.length;
    }
    static long min(long[] a) {
        long m = a[0]; for (long v : a) if (v < m) m = v; return m;
    }
    static long max(long[] a) {
        long m = a[0]; for (long v : a) if (v > m) m = v; return m;
    }
    static double std(long[] a) {
        double mean = avg(a), sq = 0;
        for (long v : a) sq += (v - mean) * (v - mean);
        return Math.sqrt(sq / a.length);
    }

    static void writeResults(String file, String label,
            long[] pointL, long[] rangeL, long[] knnL,
            long[] pointN, long[] rangeN, long[] knnN) throws IOException {

        StringBuilder sb = new StringBuilder();
        sb.append("=".repeat(70)).append("\n");
        sb.append("QUERY BENCHMARK COMPARISON — ").append(label).append("\n");
        sb.append("LiLIS runs=").append(LILIS_RUNS)
          .append("  |  NoIndex runs=").append(NO_IDX_RUNS)
          .append("  |  kNN k=").append(KNN_K).append("\n");
        sb.append("Query points: 50 random via takeSample(seed=42)\n");
        sb.append("Mode: SEQUENTIAL — no memory contention\n");
        sb.append("=".repeat(70)).append("\n\n");

        sb.append(String.format(Locale.ROOT,
                "%-25s %10s %10s %10s %10s%n",
                "Query Type", "Avg(ms)", "Min(ms)", "Max(ms)", "Std(ms)"));
        sb.append("-".repeat(70)).append("\n");

        appendRow(sb, "Point  LiLIS-K",  pointL);
        appendRow(sb, "Point  No-index", pointN);
        sb.append("\n");
        appendRow(sb, "Range  LiLIS-K",  rangeL);
        appendRow(sb, "Range  No-index", rangeN);
        sb.append("\n");
        appendRow(sb, "kNN    LiLIS-K",  knnL);
        appendRow(sb, "kNN    No-index", knnN);

        sb.append("\n").append("=".repeat(70)).append("\n");
        sb.append("SPEEDUP — No-index / LiLIS-K\n");
        sb.append("-".repeat(70)).append("\n");
        sb.append(String.format(Locale.ROOT,
                "  Point query : %.1fx faster%n", avg(pointN) / avg(pointL)));
        sb.append(String.format(Locale.ROOT,
                "  Range query : %.1fx faster%n", avg(rangeN) / avg(rangeL)));
        sb.append(String.format(Locale.ROOT,
                "  kNN   query : %.1fx faster%n", avg(knnN)   / avg(knnL)));

        sb.append("\n").append("=".repeat(70)).append("\n");
        sb.append("Paper reference (Section V.B Table III, NYC):\n");
        sb.append("  Takeaway 1: LiLIS nhanh hơn 2-3 orders of magnitude vs Sedona\n");
        sb.append("  LiLIS-K absolute (paper cluster):\n");
        sb.append("    Point :  82.59 ms\n");
        sb.append("    Range : 468.64 ms\n");
        sb.append("    kNN   : 650.20 ms\n");

        System.out.println("\n" + sb);
        try (BufferedWriter w = new BufferedWriter(new FileWriter(file))) {
            w.write(sb.toString());
        }
        System.out.println("Saved: " + file);
    }

    static void appendRow(StringBuilder sb, String lbl, long[] arr) {
        sb.append(String.format(Locale.ROOT,
                "%-25s %10.1f %10d %10d %10.1f%n",
                lbl, avg(arr), min(arr), max(arr), std(arr)));
    }
}

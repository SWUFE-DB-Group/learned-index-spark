package idnexbuild;

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import java.util.Locale;

/**
 * QueryBenchmark — Phase 3.
 *
 * Wrap đúng code tác giả (PointQuery, RangeQuery, KNNQuery),
 * chạy 50 runs, ghi avg/min/max ra file.
 *
 * Memory strategy đúng cho 11GB RAM:
 *
 *   1. persist pointRDD (MEMORY_AND_DISK_SER)
 *      → cache 46.5M điểm dạng nén để build nhanh
 *
 *   2. khai báo partitionRDD + splineRDD (lazy, chưa thực thi)
 *
 *   3. persist splineRDD (MEMORY_AND_DISK_SER) — DECLARE persistence
 *
 *   4. splineRDD.count() — TRIGGER build và cache splineRDD
 *      (lúc này pointRDD vẫn còn trong cache, build nhanh)
 *
 *   5. pointRDD.unpersist() — SAU KHI splineRDD đã materialize
 *      (giải phóng ~3-5GB RAM)
 *
 *   6. Query 50 runs — chỉ splineRDD trong RAM, không recompute
 *
 * KEY INSIGHT: bước 4 phải chạy TRƯỚC bước 5, không thì Spark sẽ
 * đọc lại CSV khi build index.
 */
public class QueryBenchmark {

    static final int   RUNS        = 50;
    static final int   WARMUP      = 3;
    static final int   KNN_K       = 10;

    // Query point cố định — tọa độ Manhattan, tồn tại trong NYC dataset
    static final Point QUERY_POINT = new Point(-73.9857, 40.7484);

    // Range query nhỏ quanh query point (~0.00001% selectivity theo paper)
    static final Rectangle QUERY_RANGE = new Rectangle(
            new Point(-73.9870, 40.7470),
            new Point(-73.9844, 40.7498)
    );

    // NYC stats cố định — từ results/figures/nyc_xy_stats.txt
    static final double TOTAL_AREA = (-72.1965 - (-74.9996)) * (41.9237 - 40.0081);
    static final long   TOTAL_N    = 46_475_157L;

    public static void main(String[] args) throws Exception {

        String datasetLabel = args[0];
        String dataPath     = args[1];
        String outputFile   = "results/" + datasetLabel + "_query_benchmark.txt";

        // ── Spark setup — giống hệt tác giả ──
        SparkConf conf = new SparkConf().setAppName("SparkApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ──────────────────────────────────────────────────────────
        // BƯỚC 1: Cache raw data dạng nén để build nhanh
        // ──────────────────────────────────────────────────────────
        JavaRDD<Point> pointRDD = PointRDDUtils.CreatePointRDD(sc, dataPath, 0);
        pointRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
        long rawCount = pointRDD.count();
        System.out.println("[QueryBenchmark] Raw data cached (SER): " + rawCount + " points");

        // ──────────────────────────────────────────────────────────
        // BƯỚC 2-3: Khai báo lineage (lazy) và DECLARE persist cho splineRDD
        // ──────────────────────────────────────────────────────────
        System.out.println("[QueryBenchmark] Building LiLIS-K index...");
        JavaRDD<Point> partitionRDD = SpatialPartition.KDBTreePartitioner(pointRDD);
        JavaRDD<Spline> splineRDD   = BuildIndex.indexBuild(partitionRDD);
        splineRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        // ──────────────────────────────────────────────────────────
        // BƯỚC 4: TRIGGER build — phải chạy TRƯỚC khi unpersist pointRDD
        // Lúc này Spark đọc pointRDD từ cache (nhanh), build index, cache splineRDD
        // ──────────────────────────────────────────────────────────
        long splineCount = splineRDD.count();
        System.out.println("[QueryBenchmark] Index CACHED (SER): " + splineCount + " partitions");

        // ──────────────────────────────────────────────────────────
        // BƯỚC 5: SAU KHI splineRDD materialize, mới xả pointRDD an toàn
        // ──────────────────────────────────────────────────────────
        pointRDD.unpersist();
        System.out.println("[QueryBenchmark] pointRDD released. Ready for queries.");

        // ──────────────────────────────────────────────────────────
        // BƯỚC 6: Benchmark queries — chỉ splineRDD trong RAM
        // ──────────────────────────────────────────────────────────
        long[] pointTimes = new long[RUNS];
        long[] rangeTimes = new long[RUNS];
        long[] knnTimes   = new long[RUNS];

        // Warm-up — không ghi kết quả
        System.out.println("[QueryBenchmark] Warming up " + WARMUP + " runs...");
        for (int w = 0; w < WARMUP; w++) {
            PointQuery.SpatialPointQuery(splineRDD, QUERY_POINT).count();
            RangeQuery.SpatialRangeQuery(splineRDD, QUERY_RANGE).count();
            KNNQuery.SpatialKNNQuery(splineRDD, KNN_K, QUERY_POINT, TOTAL_AREA, TOTAL_N);
        }
        System.out.println("[QueryBenchmark] Warm-up done. Starting timed runs...");

        // 50 timed runs — gọi đúng method của tác giả
        for (int i = 0; i < RUNS; i++) {
            long t0, t1;

            // Point query — tác giả: PointQuery.SpatialPointQuery
            t0 = System.currentTimeMillis();
            PointQuery.SpatialPointQuery(splineRDD, QUERY_POINT).count();
            t1 = System.currentTimeMillis();
            pointTimes[i] = t1 - t0;

            // Range query — tác giả: RangeQuery.SpatialRangeQuery
            t0 = System.currentTimeMillis();
            RangeQuery.SpatialRangeQuery(splineRDD, QUERY_RANGE).count();
            t1 = System.currentTimeMillis();
            rangeTimes[i] = t1 - t0;

            // kNN query — tác giả: KNNQuery.SpatialKNNQuery
            t0 = System.currentTimeMillis();
            KNNQuery.SpatialKNNQuery(splineRDD, KNN_K, QUERY_POINT, TOTAL_AREA, TOTAL_N);
            t1 = System.currentTimeMillis();
            knnTimes[i] = t1 - t0;

            if ((i + 1) % 10 == 0) {
                System.out.printf("[QueryBenchmark] Run %d/%d | point=%dms range=%dms knn=%dms%n",
                        i + 1, RUNS, pointTimes[i], rangeTimes[i], knnTimes[i]);
            }
        }

        splineRDD.unpersist();
        sc.close();

        writeResults(outputFile, datasetLabel, pointTimes, rangeTimes, knnTimes);
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
            long[] point, long[] range, long[] knn) throws IOException {

        StringBuilder sb = new StringBuilder();
        sb.append("=".repeat(55)).append("\n");
        sb.append("QUERY BENCHMARK — ").append(label).append("\n");
        sb.append("Runs=").append(RUNS).append("  kNN k=").append(KNN_K).append("\n");
        sb.append("=".repeat(55)).append("\n\n");

        sb.append(String.format(Locale.ROOT,
                "%-20s %8s %8s %8s %8s%n",
                "Query", "Avg(ms)", "Min(ms)", "Max(ms)", "Std(ms)"));
        sb.append("-".repeat(55)).append("\n");

        appendRow(sb, "Point  LiLIS-K", point);
        appendRow(sb, "Range  LiLIS-K", range);
        appendRow(sb, "kNN    LiLIS-K", knn);

        sb.append("\n").append("=".repeat(55)).append("\n");
        sb.append("Paper reference (Table III, LiLIS-K NYC):\n");
        sb.append("  Point :  82.59 ms\n");
        sb.append("  Range : 468.64 ms\n");
        sb.append("  kNN   : 650.20 ms\n");

        System.out.println("\n" + sb);
        try (BufferedWriter w = new BufferedWriter(new FileWriter(file))) {
            w.write(sb.toString());
        }
        System.out.println("Saved: " + file);
    }

    static void appendRow(StringBuilder sb, String lbl, long[] arr) {
        sb.append(String.format(Locale.ROOT,
                "%-20s %8.1f %8d %8d %8.1f%n",
                lbl, avg(arr), min(arr), max(arr), std(arr)));
    }
}
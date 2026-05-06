package benchmark;

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import partitions.SpatialPartition;
import query.DistanceQuery;
import query.KNNQuery;
import query.PointQuery;
import query.RangeQuery;
import spline.Spline;
import utils.HDFSPointReader;
import utils.Utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 统一入口: 顺序运行所有分区策略的基准测试并输出对比结果
 *
 * 用法:
 *   spark-submit --class benchmark.BenchmarkRunner <jar> --generate 50000
 *   spark-submit --class benchmark.BenchmarkRunner <jar> hdfs:///data/points.csv
 *   spark-submit --class benchmark.BenchmarkRunner <jar> --partition QuadTree --generate 50000
 */
public class BenchmarkRunner {

    private static final int WARMUP_RUNS = 1;
    private static final int MEASURE_RUNS = 3;

    public static void main(String[] args) throws Exception {
        String dataPath = null;
        int generateCount = 50000;
        boolean useGenerated = false;
        String outputPath = null;
        String targetPartition = null;

        for (int i = 0; i < args.length; i++) {
            if ("--generate".equals(args[i]) && i + 1 < args.length) {
                useGenerated = true;
                generateCount = Integer.parseInt(args[++i]);
            } else if ("--partition".equals(args[i]) && i + 1 < args.length) {
                targetPartition = args[++i];
            } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                outputPath = args[++i];
            } else if (dataPath == null) {
                dataPath = args[i];
            }
        }

        if (dataPath == null && !useGenerated) {
            useGenerated = true;
        }

        SparkConf conf = new SparkConf()
                .setAppName("LiLIS-BenchmarkRunner");
        if (isLocalMode()) {
            conf.setMaster("local[*]");
            conf.set("spark.driver.host", "localhost");
        }
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        try {
            // Load data
            JavaRDD<Point> pointRDD;
            if (useGenerated) {
                List<Point> points = generateTestData(generateCount, 42);
                pointRDD = sc.parallelize(points, Runtime.getRuntime().availableProcessors());
            } else {
                pointRDD = HDFSPointReader.readCSV(sc, dataPath);
            }

            long dataCount = pointRDD.count();
            List<Point> allPoints = pointRDD.collect();
            Rectangle boundingBox = Utils.getBoundingBox(allPoints);

            System.out.println("╔══════════════════════════════════════════════════════════════╗");
            System.out.println("║        LiLIS Benchmark Runner - All Partitions              ║");
            System.out.println("╚══════════════════════════════════════════════════════════════╝");
            System.out.println("数据量: " + dataCount + " | 边界框: " + boundingBox);
            System.out.println();

            // Partition methods
            String[] partitionNames = {"QuadTree", "KDBTree", "RTree", "FixGrid", "AdaptiveGrid"};
            List<String[]> allResults = new ArrayList<>();
            allResults.add(new String[]{"partition", "query_type", "parameter", "result_count", "time_ms", "memory_mb"});

            for (String pName : partitionNames) {
                if (targetPartition != null && !targetPartition.equalsIgnoreCase(pName)) {
                    continue;
                }

                System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                System.out.println("  分区策略: " + pName);
                System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

                try {
                    // Partition + Index
                    long t0 = System.currentTimeMillis();
                    JavaRDD<Point> partitioned = doPartition(pointRDD, pName);
                    JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);
                    indexRDD.cache();
                    long indexCount = indexRDD.count();
                    long t1 = System.currentTimeMillis();

                    System.out.println("  索引构建: " + (t1 - t0) + " ms, 分区数: " + indexCount);
                    allResults.add(new String[]{pName, "IndexBuild", "partitions=" + indexCount,
                            String.valueOf(indexCount), String.valueOf(t1 - t0), String.format("%.1f", getUsedMemoryMB())});

                    // Range Query
                    runRangeQueries(pName, indexRDD, boundingBox, allResults);

                    // Point Query
                    runPointQueries(pName, indexRDD, allPoints, allResults);

                    // KNN Query
                    runKNNQueries(pName, indexRDD, boundingBox, dataCount, allResults);

                    // Distance Query
                    runDistanceQueries(pName, indexRDD, boundingBox, allResults);

                    indexRDD.unpersist();

                } catch (Exception e) {
                    System.out.println("  [ERROR] " + pName + " 失败: " + e.getMessage());
                    allResults.add(new String[]{pName, "ERROR", e.getMessage(), "0", "0", "0"});
                }
                System.out.println();
            }

            // Output summary table
            printSummaryTable(allResults);

            // Write CSV
            if (outputPath == null) {
                outputPath = "benchmark_all_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".csv";
            }
            writeCSV(outputPath, allResults);
            System.out.println("\n结果已写入: " + outputPath);

        } finally {
            sc.close();
        }
    }

    private static JavaRDD<Point> doPartition(JavaRDD<Point> pointRDD, String method) throws Exception {
        switch (method) {
            case "QuadTree": return SpatialPartition.QuadtreePartitioner(pointRDD);
            case "KDBTree": return SpatialPartition.KDBTreePartitioner(pointRDD);
            case "RTree": return SpatialPartition.RtreePartitoner(pointRDD);
            case "FixGrid": return SpatialPartition.FixGridPartitioner(pointRDD);
            case "AdaptiveGrid": return SpatialPartition.AdaptiveGridPartitioner(pointRDD);
            default: throw new IllegalArgumentException("Unknown: " + method);
        }
    }

    private static void runRangeQueries(String pName, JavaRDD<Spline> indexRDD,
                                         Rectangle bbox, List<String[]> results) {
        double rangeX = bbox.getTo().getX() - bbox.getFrom().getX();
        double rangeY = bbox.getTo().getY() - bbox.getFrom().getY();
        double cx = (bbox.getFrom().getX() + bbox.getTo().getX()) / 2;
        double cy = (bbox.getFrom().getY() + bbox.getTo().getY()) / 2;

        double[] selectivities = {0.01, 0.05, 0.1, 0.25};
        for (double sel : selectivities) {
            double halfW = rangeX * Math.sqrt(sel) / 2;
            double halfH = rangeY * Math.sqrt(sel) / 2;
            Rectangle qr = new Rectangle(cx - halfW, cy - halfH, cx + halfW, cy + halfH);

            // warmup
            for (int w = 0; w < WARMUP_RUNS; w++) {
                RangeQuery.SpatialRangeQuery(indexRDD, qr).count();
            }

            long totalTime = 0;
            long count = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long t0 = System.currentTimeMillis();
                count = RangeQuery.SpatialRangeQuery(indexRDD, qr).count();
                totalTime += System.currentTimeMillis() - t0;
            }
            long avg = totalTime / MEASURE_RUNS;
            String param = String.format("sel=%.0f%%", sel * 100);
            System.out.println("  Range " + param + " | " + count + " 条 | " + avg + " ms");
            results.add(new String[]{pName, "RangeQuery", param, String.valueOf(count),
                    String.valueOf(avg), String.format("%.1f", getUsedMemoryMB())});
        }
    }

    private static void runPointQueries(String pName, JavaRDD<Spline> indexRDD,
                                         List<Point> allPoints, List<String[]> results) {
        Random rng = new Random(99);
        int queryCount = 20;

        // warmup
        PointQuery.SpatialPointQuery(indexRDD, allPoints.get(0)).count();

        long totalTime = 0;
        int found = 0;
        for (int i = 0; i < queryCount; i++) {
            Point target = allPoints.get(rng.nextInt(Math.min(allPoints.size(), 1000)));
            long t0 = System.currentTimeMillis();
            long cnt = PointQuery.SpatialPointQuery(indexRDD, target).count();
            totalTime += System.currentTimeMillis() - t0;
            if (cnt > 0) found++;
        }
        long avg = totalTime / queryCount;
        System.out.println("  Point | 命中: " + found + "/" + queryCount + " | 平均: " + avg + " ms");
        results.add(new String[]{pName, "PointQuery", "count=" + queryCount, String.valueOf(found),
                String.valueOf(avg), String.format("%.1f", getUsedMemoryMB())});
    }

    private static void runKNNQueries(String pName, JavaRDD<Spline> indexRDD,
                                       Rectangle bbox, long dataCount, List<String[]> results) {
        double cx = (bbox.getFrom().getX() + bbox.getTo().getX()) / 2;
        double cy = (bbox.getFrom().getY() + bbox.getTo().getY()) / 2;
        Point center = new Point(cx, cy);
        double maxArea = bbox.getArea();

        int[] kValues = {1, 5, 10, 50, 100};

        // warmup
        KNNQuery.SpatialKNNQuery(indexRDD, 5, center, maxArea, dataCount);

        for (int k : kValues) {
            long totalTime = 0;
            int resultSize = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long t0 = System.currentTimeMillis();
                resultSize = KNNQuery.SpatialKNNQuery(indexRDD, k, center, maxArea, dataCount).size();
                totalTime += System.currentTimeMillis() - t0;
            }
            long avg = totalTime / MEASURE_RUNS;
            System.out.println("  KNN K=" + k + " | " + resultSize + " 条 | " + avg + " ms");
            results.add(new String[]{pName, "KNNQuery", "K=" + k, String.valueOf(resultSize),
                    String.valueOf(avg), String.format("%.1f", getUsedMemoryMB())});
        }
    }

    private static void runDistanceQueries(String pName, JavaRDD<Spline> indexRDD,
                                            Rectangle bbox, List<String[]> results) {
        double cx = (bbox.getFrom().getX() + bbox.getTo().getX()) / 2;
        double cy = (bbox.getFrom().getY() + bbox.getTo().getY()) / 2;
        Point center = new Point(cx, cy);

        double[] distances = {500, 1000, 2000, 5000, 10000};

        // warmup
        DistanceQuery.SpatialDistanceQuery(indexRDD, center, 1000).count();

        for (double dist : distances) {
            long totalTime = 0;
            long count = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long t0 = System.currentTimeMillis();
                count = DistanceQuery.SpatialDistanceQuery(indexRDD, center, dist).count();
                totalTime += System.currentTimeMillis() - t0;
            }
            long avg = totalTime / MEASURE_RUNS;
            System.out.println("  Distance " + (int) dist + "m | " + count + " 条 | " + avg + " ms");
            results.add(new String[]{pName, "DistanceQuery", "dist=" + (int) dist + "m",
                    String.valueOf(count), String.valueOf(avg), String.format("%.1f", getUsedMemoryMB())});
        }
    }

    private static void printSummaryTable(List<String[]> results) {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                         性能对比汇总                                        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");
        System.out.printf("%-14s %-14s %-12s %-12s %-10s %-10s%n",
                "Partition", "Query", "Param", "Results", "Time(ms)", "Mem(MB)");
        System.out.println("─────────────────────────────────────────────────────────────────────────────");
        for (int i = 1; i < results.size(); i++) {
            String[] row = results.get(i);
            System.out.printf("%-14s %-14s %-12s %-12s %-10s %-10s%n",
                    row[0], row[1], row[2], row[3], row[4], row[5]);
        }
    }

    private static List<Point> generateTestData(int n, long seed) {
        Random rng = new Random(seed);
        List<Point> points = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double x = -87.9 + rng.nextDouble() * 0.5;
            double y = 41.6 + rng.nextDouble() * 0.4;
            points.add(new Point(x, y));
        }
        return points;
    }

    private static double getUsedMemoryMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0);
    }

    private static boolean isLocalMode() {
        String master = System.getenv("SPARK_MASTER");
        return master == null || master.isEmpty();
    }

    private static void writeCSV(String path, List<String[]> results) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            for (String[] row : results) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("写入结果失败: " + e.getMessage());
        }
    }
}

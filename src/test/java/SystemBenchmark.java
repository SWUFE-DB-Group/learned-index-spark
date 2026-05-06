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
 * LiLIS 系统完整流程测试与性能基准
 *
 * 测试流程: 数据读取 → 空间分区 → 索引构建 → 查询执行
 * 记录: 各阶段耗时、查询结果数量、峰值内存使用
 *
 * 用法:
 *   # 使用 HDFS 数据
 *   spark-submit --class SystemBenchmark target/LearnIndexSpark-1.0-SNAPSHOT.jar hdfs:///data/points.csv [输出文件路径]
 *
 *   # 使用生成的测试数据 (不指定数据路径)
 *   spark-submit --class SystemBenchmark target/LearnIndexSpark-1.0-SNAPSHOT.jar --generate 100000 [输出文件路径]
 *
 *   # 本地模式运行
 *   java -cp "target/classes:$(mvn dependency:build-classpath ...)" SystemBenchmark --generate 50000
 */
public class SystemBenchmark {

    private static final List<String> logs = new ArrayList<>();
    private static long benchmarkStartTime;

    public static void main(String[] args) throws Exception {
        benchmarkStartTime = System.currentTimeMillis();

        String dataPath = null;
        int generateCount = 50000;
        String outputPath = null;
        boolean useGenerated = false;

        for (int i = 0; i < args.length; i++) {
            if ("--generate".equals(args[i]) && i + 1 < args.length) {
                useGenerated = true;
                generateCount = Integer.parseInt(args[++i]);
            } else if (dataPath == null) {
                dataPath = args[i];
            } else {
                outputPath = args[i];
            }
        }

        if (dataPath == null && !useGenerated) {
            useGenerated = true;
        }

        log("╔══════════════════════════════════════════════════════════════╗");
        log("║          LiLIS System Benchmark                            ║");
        log("╚══════════════════════════════════════════════════════════════╝");
        log("");
        log("启动时间: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        log("数据源: " + (useGenerated ? "生成测试数据 (" + generateCount + " 点)" : dataPath));
        log("");

        SparkConf conf = new SparkConf()
                .setAppName("LiLIS-SystemBenchmark");

        if (isLocalMode()) {
            conf.setMaster("local[*]");
            conf.set("spark.driver.host", "localhost");
        }
        conf.set("spark.ui.enabled", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        try {
            // ========== 1. 数据读取 ==========
            logSection("1. 数据读取");
            long memBefore = getUsedMemory();
            long t0 = System.currentTimeMillis();

            JavaRDD<Point> pointRDD;
            if (useGenerated) {
                List<Point> points = generateTestData(generateCount, 42);
                pointRDD = sc.parallelize(points, Runtime.getRuntime().availableProcessors());
            } else {
                pointRDD = HDFSPointReader.readCSV(sc, dataPath);
            }

            long dataCount = pointRDD.count();
            long t1 = System.currentTimeMillis();
            long memAfterLoad = getUsedMemory();

            log("  数据量: " + dataCount + " 条");
            log("  读取耗时: " + (t1 - t0) + " ms");
            log("  内存使用: " + formatMemory(memAfterLoad - memBefore));

            // 计算边界框
            List<Point> samplePoints = pointRDD.take(100);
            Rectangle boundingBox = Utils.getBoundingBox(pointRDD.collect());
            log("  边界框: " + boundingBox);
            log("");

            // ========== 2. 空间分区 ==========
            logSection("2. 空间分区");
            String[] partitionMethods = {"QuadTree", "KDBTree", "RTree"};

            JavaRDD<Point> partitionedRDD = null;
            for (String method : partitionMethods) {
                System.gc();
                long pt0 = System.currentTimeMillis();
                try {
                    JavaRDD<Point> pRDD = partition(pointRDD, method);
                    long partCount = pRDD.getNumPartitions();
                    long pt1 = System.currentTimeMillis();
                    log("  [" + method + "] 分区数: " + partCount + ", 耗时: " + (pt1 - pt0) + " ms");
                    if (partitionedRDD == null) {
                        partitionedRDD = pRDD;
                    }
                } catch (Exception e) {
                    log("  [" + method + "] 失败: " + e.getMessage());
                }
            }
            log("");

            // ========== 3. 索引构建 ==========
            logSection("3. 索引构建");
            System.gc();
            long memBeforeIndex = getUsedMemory();
            long it0 = System.currentTimeMillis();

            JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitionedRDD);
            indexRDD.cache();
            long indexCount = indexRDD.count();

            long it1 = System.currentTimeMillis();
            long memAfterIndex = getUsedMemory();

            log("  索引分区数: " + indexCount);
            log("  构建耗时: " + (it1 - it0) + " ms");
            log("  索引内存增量: " + formatMemory(memAfterIndex - memBeforeIndex));

            // 统计索引信息
            List<Spline> splineSample = indexRDD.take(3);
            for (int i = 0; i < splineSample.size(); i++) {
                Spline s = splineSample.get(i);
                log("  分区[" + i + "]: points=" + s.size() +
                        ", splinePoints=" + (s.getSpline() != null ? s.getSpline().size() : 0) +
                        ", linearScan=" + s.isLinear_scan());
            }
            log("");

            // ========== 4. 查询执行 ==========
            logSection("4. 查询执行");

            double maxArea = boundingBox.getArea();
            double centerX = (boundingBox.getFrom().getX() + boundingBox.getTo().getX()) / 2;
            double centerY = (boundingBox.getFrom().getY() + boundingBox.getTo().getY()) / 2;
            double rangeX = boundingBox.getTo().getX() - boundingBox.getFrom().getX();
            double rangeY = boundingBox.getTo().getY() - boundingBox.getFrom().getY();

            // 4.1 范围查询
            log("  --- 4.1 范围查询 (Range Query) ---");
            double[] selectivities = {0.01, 0.05, 0.1, 0.25};
            for (double sel : selectivities) {
                double halfW = rangeX * Math.sqrt(sel) / 2;
                double halfH = rangeY * Math.sqrt(sel) / 2;
                Rectangle queryRect = new Rectangle(
                        centerX - halfW, centerY - halfH,
                        centerX + halfW, centerY + halfH);

                System.gc();
                long qt0 = System.currentTimeMillis();
                JavaRDD<Point> result = RangeQuery.SpatialRangeQuery(indexRDD, queryRect);
                long count = result.count();
                long qt1 = System.currentTimeMillis();
                long peakMem = getUsedMemory();

                log("  选择率=" + String.format("%.0f%%", sel * 100) +
                        " | 结果: " + count + " 条" +
                        " | 耗时: " + (qt1 - qt0) + " ms" +
                        " | 内存: " + formatMemory(peakMem));
            }
            log("");

            // 4.2 点查询
            log("  --- 4.2 点查询 (Point Query) ---");
            Random rng = new Random(99);
            List<Point> allPoints = pointRDD.take(1000);
            long totalPointQueryTime = 0;
            int pointQueryCount = 10;
            int foundCount = 0;

            for (int i = 0; i < pointQueryCount; i++) {
                Point target = allPoints.get(rng.nextInt(allPoints.size()));
                long qt0 = System.currentTimeMillis();
                JavaRDD<Point> result = PointQuery.SpatialPointQuery(indexRDD, target);
                long count = result.count();
                long qt1 = System.currentTimeMillis();
                totalPointQueryTime += (qt1 - qt0);
                if (count > 0) foundCount++;
            }
            log("  查询次数: " + pointQueryCount +
                    " | 命中: " + foundCount +
                    " | 总耗时: " + totalPointQueryTime + " ms" +
                    " | 平均: " + (totalPointQueryTime / pointQueryCount) + " ms/次");
            log("");

            // 4.3 KNN 查询
            log("  --- 4.3 KNN 查询 ---");
            int[] kValues = {1, 5, 10, 50};
            Point knnCenter = new Point(centerX, centerY);

            for (int k : kValues) {
                System.gc();
                long qt0 = System.currentTimeMillis();
                List<Point> knnResult = KNNQuery.SpatialKNNQuery(
                        indexRDD, k, knnCenter, maxArea, dataCount);
                long qt1 = System.currentTimeMillis();
                long peakMem = getUsedMemory();

                double maxDist = 0;
                for (Point p : knnResult) {
                    double d = knnCenter.distanceTo(p);
                    if (d > maxDist) maxDist = d;
                }

                log("  K=" + k +
                        " | 结果: " + knnResult.size() + " 条" +
                        " | 最大距离: " + String.format("%.6f", maxDist) +
                        " | 耗时: " + (qt1 - qt0) + " ms" +
                        " | 内存: " + formatMemory(peakMem));
            }
            log("");

            // 4.4 距离查询 (注意: DistanceQuery 内部使用 Haversine 距离, 单位为米)
            log("  --- 4.4 距离查询 (Distance Query, Haversine 米) ---");
            double[] distances = {500, 2000, 5000};
            Point distCenter = new Point(centerX, centerY);

            for (double dist : distances) {
                System.gc();
                long qt0 = System.currentTimeMillis();
                JavaRDD<Point> result = DistanceQuery.SpatialDistanceQuery(indexRDD, distCenter, dist);
                long count = result.count();
                long qt1 = System.currentTimeMillis();

                log("  距离=" + String.format("%.4f", dist) +
                        " | 结果: " + count + " 条" +
                        " | 耗时: " + (qt1 - qt0) + " ms");
            }
            log("");

            // ========== 5. 汇总 ==========
            logSection("5. 性能汇总");
            long totalTime = System.currentTimeMillis() - benchmarkStartTime;
            long peakMemory = getUsedMemory();

            log("  总运行时间: " + totalTime + " ms (" + String.format("%.2f", totalTime / 1000.0) + " s)");
            log("  JVM 峰值内存: " + formatMemory(peakMemory));
            log("  JVM 最大可用: " + formatMemory(Runtime.getRuntime().maxMemory()));
            log("  CPU 核心数: " + Runtime.getRuntime().availableProcessors());
            log("");
            log("══════════════════════════════════════════════════════════════");

            // 输出到文件
            if (outputPath != null) {
                writeReport(outputPath);
                System.out.println("\n报告已写入: " + outputPath);
            } else {
                String defaultOutput = "benchmark_result_" +
                        new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
                writeReport(defaultOutput);
                System.out.println("\n报告已写入: " + defaultOutput);
            }

        } finally {
            sc.close();
        }
    }

    // ==================== 辅助方法 ====================

    private static JavaRDD<Point> partition(JavaRDD<Point> pointRDD, String method) throws Exception {
        switch (method) {
            case "QuadTree":
                return SpatialPartition.QuadtreePartitioner(pointRDD);
            case "KDBTree":
                return SpatialPartition.KDBTreePartitioner(pointRDD);
            case "RTree":
                return SpatialPartition.RtreePartitoner(pointRDD);
            case "FixGrid":
                return SpatialPartition.FixGridPartitioner(pointRDD);
            case "AdaptiveGrid":
                return SpatialPartition.AdaptiveGridPartitioner(pointRDD);
            default:
                throw new IllegalArgumentException("Unknown partition method: " + method);
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

    private static long getUsedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static String formatMemory(long bytes) {
        if (bytes < 0) return "N/A";
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static boolean isLocalMode() {
        String master = System.getenv("SPARK_MASTER");
        return master == null || master.isEmpty();
    }

    private static void log(String msg) {
        System.out.println(msg);
        logs.add(msg);
    }

    private static void logSection(String title) {
        log("┌──────────────────────────────────────────────────────────────");
        log("│ " + title);
        log("└──────────────────────────────────────────────────────────────");
    }

    private static void writeReport(String path) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            for (String line : logs) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("写入报告失败: " + e.getMessage());
        }
    }
}

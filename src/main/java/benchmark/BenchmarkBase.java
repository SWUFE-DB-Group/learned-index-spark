package benchmark;

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

public abstract class BenchmarkBase {

    protected static final int WARMUP_RUNS = 1;
    protected static final int MEASURE_RUNS = 3;

    protected JavaSparkContext sc;
    protected JavaRDD<Point> pointRDD;
    protected JavaRDD<Spline> indexRDD;
    protected Rectangle boundingBox;
    protected long dataCount;
    protected List<String[]> csvResults = new ArrayList<>();

    public abstract String getPartitionName();

    public abstract JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception;

    public void run(String[] args) throws Exception {
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

        SparkConf conf = new SparkConf()
                .setAppName("LiLIS-Benchmark-" + getPartitionName());

        if (isLocalMode()) {
            conf.setMaster("local[*]");
            conf.set("spark.driver.host", "localhost");
        }
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        try {
            // CSV header
            csvResults.add(new String[]{"partition", "query_type", "parameter", "result_count", "time_ms", "memory_mb"});

            System.out.println("=== LiLIS Benchmark: " + getPartitionName() + " ===");
            System.out.println("数据源: " + (useGenerated ? "生成 " + generateCount + " 点" : dataPath));
            System.out.println();

            // 1. Load data
            if (useGenerated) {
                List<Point> points = generateTestData(generateCount, 42);
                pointRDD = sc.parallelize(points, Runtime.getRuntime().availableProcessors());
            } else {
                pointRDD = HDFSPointReader.readCSV(sc, dataPath);
            }
            dataCount = pointRDD.count();
            boundingBox = Utils.getBoundingBox(pointRDD.collect());

            // 2. Partition + Index
            System.out.println("[分区] " + getPartitionName() + " ...");
            long t0 = System.currentTimeMillis();
            JavaRDD<Point> partitionedRDD = partition(pointRDD);
            indexRDD = BuildIndex.indexBuild(partitionedRDD);
            indexRDD.cache();
            long indexCount = indexRDD.count();
            long t1 = System.currentTimeMillis();
            System.out.println("  分区+索引耗时: " + (t1 - t0) + " ms, 索引分区数: " + indexCount);
            System.out.println();

            // 3. Run queries
            benchmarkRangeQuery();
            benchmarkPointQuery();
            benchmarkKNNQuery();
            benchmarkDistanceQuery();

            // 4. Output
            if (outputPath == null) {
                outputPath = "benchmark_" + getPartitionName() + "_" +
                        new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".csv";
            }
            writeCSV(outputPath);
            System.out.println("\n结果已写入: " + outputPath);

        } finally {
            sc.close();
        }
    }

    protected void benchmarkRangeQuery() {
        System.out.println("[Range Query]");
        double rangeX = boundingBox.getTo().getX() - boundingBox.getFrom().getX();
        double rangeY = boundingBox.getTo().getY() - boundingBox.getFrom().getY();
        double centerX = (boundingBox.getFrom().getX() + boundingBox.getTo().getX()) / 2;
        double centerY = (boundingBox.getFrom().getY() + boundingBox.getTo().getY()) / 2;

        double[] selectivities = {0.01, 0.05, 0.1, 0.25};

        for (double sel : selectivities) {
            double halfW = rangeX * Math.sqrt(sel) / 2;
            double halfH = rangeY * Math.sqrt(sel) / 2;
            Rectangle queryRect = new Rectangle(
                    centerX - halfW, centerY - halfH,
                    centerX + halfW, centerY + halfH);

            String param = String.format("sel=%.0f%%", sel * 100);

            // warmup
            for (int w = 0; w < WARMUP_RUNS; w++) {
                RangeQuery.SpatialRangeQuery(indexRDD, queryRect).count();
            }

            // measure
            long totalTime = 0;
            long resultCount = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long qt0 = System.currentTimeMillis();
                JavaRDD<Point> result = RangeQuery.SpatialRangeQuery(indexRDD, queryRect);
                resultCount = result.count();
                long qt1 = System.currentTimeMillis();
                totalTime += (qt1 - qt0);
            }
            long avgTime = totalTime / MEASURE_RUNS;
            double memMB = getUsedMemoryMB();

            System.out.println("  " + param + " | 结果: " + resultCount + " | 平均耗时: " + avgTime + " ms");
            csvResults.add(new String[]{getPartitionName(), "RangeQuery", param,
                    String.valueOf(resultCount), String.valueOf(avgTime), String.format("%.1f", memMB)});
        }
        System.out.println();
    }

    protected void benchmarkPointQuery() {
        System.out.println("[Point Query]");
        Random rng = new Random(99);
        List<Point> samplePoints = pointRDD.take(1000);
        int queryCount = 20;

        // warmup
        for (int w = 0; w < WARMUP_RUNS; w++) {
            Point target = samplePoints.get(0);
            PointQuery.SpatialPointQuery(indexRDD, target).count();
        }

        // measure
        long totalTime = 0;
        int foundCount = 0;
        for (int i = 0; i < queryCount; i++) {
            Point target = samplePoints.get(rng.nextInt(samplePoints.size()));
            long qt0 = System.currentTimeMillis();
            JavaRDD<Point> result = PointQuery.SpatialPointQuery(indexRDD, target);
            long count = result.count();
            long qt1 = System.currentTimeMillis();
            totalTime += (qt1 - qt0);
            if (count > 0) foundCount++;
        }
        long avgTime = totalTime / queryCount;
        double memMB = getUsedMemoryMB();

        System.out.println("  查询次数: " + queryCount + " | 命中: " + foundCount + " | 平均: " + avgTime + " ms/次");
        csvResults.add(new String[]{getPartitionName(), "PointQuery", "count=" + queryCount,
                String.valueOf(foundCount), String.valueOf(avgTime), String.format("%.1f", memMB)});
        System.out.println();
    }

    protected void benchmarkKNNQuery() {
        System.out.println("[KNN Query]");
        double centerX = (boundingBox.getFrom().getX() + boundingBox.getTo().getX()) / 2;
        double centerY = (boundingBox.getFrom().getY() + boundingBox.getTo().getY()) / 2;
        Point knnCenter = new Point(centerX, centerY);
        double maxArea = boundingBox.getArea();

        int[] kValues = {1, 5, 10, 50, 100};

        // warmup
        for (int w = 0; w < WARMUP_RUNS; w++) {
            KNNQuery.SpatialKNNQuery(indexRDD, 5, knnCenter, maxArea, dataCount);
        }

        for (int k : kValues) {
            long totalTime = 0;
            int resultSize = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long qt0 = System.currentTimeMillis();
                List<Point> knnResult = KNNQuery.SpatialKNNQuery(indexRDD, k, knnCenter, maxArea, dataCount);
                resultSize = knnResult.size();
                long qt1 = System.currentTimeMillis();
                totalTime += (qt1 - qt0);
            }
            long avgTime = totalTime / MEASURE_RUNS;
            double memMB = getUsedMemoryMB();

            System.out.println("  K=" + k + " | 结果: " + resultSize + " | 平均耗时: " + avgTime + " ms");
            csvResults.add(new String[]{getPartitionName(), "KNNQuery", "K=" + k,
                    String.valueOf(resultSize), String.valueOf(avgTime), String.format("%.1f", memMB)});
        }
        System.out.println();
    }

    protected void benchmarkDistanceQuery() {
        System.out.println("[Distance Query]");
        double centerX = (boundingBox.getFrom().getX() + boundingBox.getTo().getX()) / 2;
        double centerY = (boundingBox.getFrom().getY() + boundingBox.getTo().getY()) / 2;
        Point distCenter = new Point(centerX, centerY);

        double[] distances = {500, 1000, 2000, 5000, 10000};

        // warmup
        for (int w = 0; w < WARMUP_RUNS; w++) {
            DistanceQuery.SpatialDistanceQuery(indexRDD, distCenter, 1000).count();
        }

        for (double dist : distances) {
            long totalTime = 0;
            long resultCount = 0;
            for (int r = 0; r < MEASURE_RUNS; r++) {
                System.gc();
                long qt0 = System.currentTimeMillis();
                JavaRDD<Point> result = DistanceQuery.SpatialDistanceQuery(indexRDD, distCenter, dist);
                resultCount = result.count();
                long qt1 = System.currentTimeMillis();
                totalTime += (qt1 - qt0);
            }
            long avgTime = totalTime / MEASURE_RUNS;
            double memMB = getUsedMemoryMB();

            System.out.println("  距离=" + (int) dist + "m | 结果: " + resultCount + " | 平均耗时: " + avgTime + " ms");
            csvResults.add(new String[]{getPartitionName(), "DistanceQuery", "dist=" + (int) dist + "m",
                    String.valueOf(resultCount), String.valueOf(avgTime), String.format("%.1f", memMB)});
        }
        System.out.println();
    }

    // ==================== Utilities ====================

    protected static List<Point> generateTestData(int n, long seed) {
        Random rng = new Random(seed);
        List<Point> points = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double x = -87.9 + rng.nextDouble() * 0.5;
            double y = 41.6 + rng.nextDouble() * 0.4;
            points.add(new Point(x, y));
        }
        return points;
    }

    protected static double getUsedMemoryMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0);
    }

    protected static boolean isLocalMode() {
        String master = System.getenv("SPARK_MASTER");
        return master == null || master.isEmpty();
    }

    protected void writeCSV(String path) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            for (String[] row : csvResults) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("写入结果失败: " + e.getMessage());
        }
    }

    public List<String[]> getResults() {
        return csvResults;
    }
}

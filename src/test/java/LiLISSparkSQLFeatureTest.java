import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sql.IndexedSpatialRDD;
import sql.LiLIS;

import java.util.Arrays;
import java.util.List;

public class LiLISSparkSQLFeatureTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== LiLIS Spark SQL Feature Test ===\n");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("LiLISSparkSQLFeatureTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        try {
            LiLIS.register(spark);
            Dataset<Row> points = testPoints(spark);
            points.createOrReplaceTempView("raw_points");

            IndexedSpatialRDD index = LiLIS.createIndexAsView(spark, points, "points", "QuadTree");

            testSQLUDFRangeFilter(spark);
            testSQLUDFPointFilter(spark);
            testSQLUDFDistanceFilter(spark);
            testIndexedDataSourceView(spark);
            testManualDataSourceView(spark);
            testIndexedRangeResultView(spark);
            testIndexedPointResultView(spark);
            testIndexedKNNResultView(spark);
            testIndexedDistanceResultView(spark);
            testIndexedDataFrameAPIMatchesSQL(spark, index);

            System.out.println("\n=== ALL LiLIS Spark SQL FEATURE TESTS PASSED ===");
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> testPoints(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                new TestPoint(-87.697249, 41.822730),
                new TestPoint(-87.686513, 41.830143),
                new TestPoint(-87.650000, 41.850000),
                new TestPoint(-87.620000, 41.870000),
                new TestPoint(-87.500000, 41.900000)
        ), TestPoint.class).select("x", "y");
    }

    private static void testSQLUDFRangeFilter(SparkSession spark) {
        System.out.print("[1] SQL UDF range filter... ");
        long count = spark.sql("SELECT * FROM raw_points WHERE lilis_in_range(x, y, -87.70, 41.82, -87.64, 41.86)").count();
        assertEquals(3, count, "SQL UDF range filter");
        System.out.println("PASSED");
    }

    private static void testSQLUDFPointFilter(SparkSession spark) {
        System.out.print("[2] SQL UDF point filter... ");
        long count = spark.sql("SELECT * FROM raw_points WHERE lilis_is_point(x, y, -87.65, 41.85)").count();
        assertEquals(1, count, "SQL UDF point filter");
        System.out.println("PASSED");
    }

    private static void testSQLUDFDistanceFilter(SparkSession spark) {
        System.out.print("[3] SQL UDF distance filter... ");
        long count = spark.sql("SELECT * FROM raw_points WHERE lilis_in_distance(x, y, -87.65, 41.85, 0.08)").count();
        assertTrue(count >= 3, "SQL UDF distance filter expected at least 3 rows, got " + count);
        System.out.println("PASSED");
    }

    private static void testIndexedDataSourceView(SparkSession spark) {
        System.out.print("[4] auto DataSource V2 indexed view... ");
        long count = spark.sql("SELECT * FROM points_lilis WHERE x >= -87.70 AND x <= -87.64 AND y >= 41.82 AND y <= 41.86").count();
        assertEquals(3, count, "auto DataSource V2 indexed view");
        System.out.println("PASSED");
    }

    private static void testManualDataSourceView(SparkSession spark) {
        System.out.print("[5] manual USING lilis view... ");
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW indexed_points USING lilis OPTIONS (index_name 'points')");
        long count = spark.sql("SELECT * FROM indexed_points WHERE x > -87.70 AND x < -87.64 AND y > 41.82 AND y < 41.86").count();
        assertEquals(3, count, "manual USING lilis view");
        System.out.println("PASSED");
    }

    private static void testIndexedRangeResultView(SparkSession spark) {
        System.out.print("[6] rangeQueryAsView SQL result... ");
        LiLIS.rangeQueryAsView(spark, "points", "range_result", -87.70, 41.82, -87.64, 41.86);
        long count = spark.sql("SELECT * FROM range_result").count();
        assertEquals(3, count, "rangeQueryAsView SQL result");
        System.out.println("PASSED");
    }

    private static void testIndexedPointResultView(SparkSession spark) {
        System.out.print("[7] pointQueryAsView SQL result... ");
        LiLIS.pointQueryAsView(spark, "points", "point_result", -87.65, 41.85);
        long count = spark.sql("SELECT * FROM point_result").count();
        assertEquals(1, count, "pointQueryAsView SQL result");
        System.out.println("PASSED");
    }

    private static void testIndexedKNNResultView(SparkSession spark) {
        System.out.print("[8] knnQueryAsView SQL result... ");
        LiLIS.knnQueryAsView(spark, "points", "knn_result", -87.65, 41.85, 2);
        long count = spark.sql("SELECT * FROM knn_result").count();
        assertEquals(2, count, "knnQueryAsView SQL result");
        System.out.println("PASSED");
    }

    private static void testIndexedDistanceResultView(SparkSession spark) {
        System.out.print("[9] distanceQueryAsView SQL result... ");
        LiLIS.distanceQueryAsView(spark, "points", "distance_result", -87.65, 41.85, 0.08);
        long count = spark.sql("SELECT * FROM distance_result").count();
        assertTrue(count >= 3, "distanceQueryAsView expected at least 3 rows, got " + count);
        System.out.println("PASSED");
    }

    private static void testIndexedDataFrameAPIMatchesSQL(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[10] DataFrame API matches indexed SQL... ");
        long apiCount = LiLIS.rangeQuery(spark, index, -87.70, 41.82, -87.64, 41.86).count();
        long sqlCount = spark.sql("SELECT * FROM points_lilis WHERE x >= -87.70 AND x <= -87.64 AND y >= 41.82 AND y <= 41.86").count();
        assertEquals(apiCount, sqlCount, "DataFrame API matches indexed SQL");
        System.out.println("PASSED");
    }

    private static void assertEquals(long expected, long actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " expected " + expected + ", got " + actual);
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static class TestPoint implements java.io.Serializable {
        private double x;
        private double y;

        public TestPoint() {
        }

        public TestPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return x;
        }

        public void setX(double x) {
            this.x = x;
        }

        public double getY() {
            return y;
        }

        public void setY(double y) {
            this.y = y;
        }
    }
}

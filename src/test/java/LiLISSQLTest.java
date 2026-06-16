import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sql.IndexedSpatialRDD;
import sql.LiLIS;

import java.util.Arrays;

public class LiLISSQLTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== LiLIS Spark SQL Test ===\n");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("LiLISSQLTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        try {
            LiLIS.register(spark);
            Dataset<Row> points = spark.createDataFrame(Arrays.asList(
                    new TestPoint(-87.697249, 41.822730),
                    new TestPoint(-87.686513, 41.830143),
                    new TestPoint(-87.650000, 41.850000),
                    new TestPoint(-87.620000, 41.870000),
                    new TestPoint(-87.500000, 41.900000)
            ), TestPoint.class).select("x", "y");
            points.createOrReplaceTempView("points");

            IndexedSpatialRDD index = LiLIS.createIndex(spark, points, "QuadTree");

            testRangeQuery(spark, index);
            testPointQuery(spark, index);
            testDistanceQuery(spark, index);
            testKNNQuery(spark, index);
            testSQLUDF(spark);
            testSpatialJoin(spark, index);

            System.out.println("\n=== ALL LiLIS Spark SQL TESTS PASSED ===");
        } finally {
            spark.stop();
        }
    }

    private static void testRangeQuery(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[1] indexed range query... ");
        long count = LiLIS.rangeQuery(spark, index, -87.70, 41.82, -87.64, 41.86).count();
        assert count == 3 : "Expected 3 range results, got " + count;
        System.out.println("PASSED");
    }

    private static void testPointQuery(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[2] indexed point query... ");
        long count = LiLIS.pointQuery(spark, index, -87.650000, 41.850000).count();
        assert count == 1 : "Expected 1 point result, got " + count;
        System.out.println("PASSED");
    }

    private static void testDistanceQuery(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[3] indexed distance query... ");
        long count = LiLIS.distanceQuery(spark, index, -87.650000, 41.850000, 0.08).count();
        assert count >= 3 : "Expected at least 3 distance results, got " + count;
        System.out.println("PASSED");
    }

    private static void testKNNQuery(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[4] indexed KNN query... ");
        long count = LiLIS.knnQuery(spark, index, -87.650000, 41.850000, 2).count();
        assert count == 2 : "Expected 2 KNN results, got " + count;
        System.out.println("PASSED");
    }

    private static void testSQLUDF(SparkSession spark) {
        System.out.print("[5] SQL UDF filter... ");
        long count = spark.sql("SELECT * FROM points WHERE lilis_in_range(x, y, -87.70, 41.82, -87.64, 41.86)").count();
        assert count == 3 : "Expected 3 SQL UDF results, got " + count;
        System.out.println("PASSED");
    }

    private static void testSpatialJoin(SparkSession spark, IndexedSpatialRDD index) {
        System.out.print("[6] indexed spatial join... ");
        Dataset<Row> polygons = spark.createDataFrame(Arrays.asList(
                new TestPolygon(1L, -87.70, 41.82, -87.64, 41.86),
                new TestPolygon(2L, -87.63, 41.86, -87.61, 41.88)
        ), TestPolygon.class).select("polygon_id", "xmin", "ymin", "xmax", "ymax");
        long count = LiLIS.spatialJoin(spark, index, polygons).count();
        assert count == 4 : "Expected 4 joined rows, got " + count;
        System.out.println("PASSED");
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

    public static class TestPolygon implements java.io.Serializable {
        private long polygon_id;
        private double xmin;
        private double ymin;
        private double xmax;
        private double ymax;

        public TestPolygon() {
        }

        public TestPolygon(long polygon_id, double xmin, double ymin, double xmax, double ymax) {
            this.polygon_id = polygon_id;
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
        }

        public long getPolygon_id() {
            return polygon_id;
        }

        public void setPolygon_id(long polygon_id) {
            this.polygon_id = polygon_id;
        }

        public double getXmin() {
            return xmin;
        }

        public void setXmin(double xmin) {
            this.xmin = xmin;
        }

        public double getYmin() {
            return ymin;
        }

        public void setYmin(double ymin) {
            this.ymin = ymin;
        }

        public double getXmax() {
            return xmax;
        }

        public void setXmax(double xmax) {
            this.xmax = xmax;
        }

        public double getYmax() {
            return ymax;
        }

        public void setYmax(double ymax) {
            this.ymax = ymax;
        }
    }
}

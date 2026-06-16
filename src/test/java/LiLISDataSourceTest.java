import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sql.LiLIS;

import java.util.Arrays;

public class LiLISDataSourceTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== LiLIS DataSource V2 Test ===\n");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("LiLISDataSourceTest")
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

            LiLIS.createIndexAsView(spark, points, "points", "QuadTree");

            testDataSourceView(spark);
            testManualDataSourceView(spark);
            testProgrammaticQueryView(spark);

            System.out.println("\n=== ALL LiLIS DataSource V2 TESTS PASSED ===");
        } finally {
            spark.stop();
        }
    }

    private static void testDataSourceView(SparkSession spark) {
        System.out.print("[1] auto indexed temp view... ");
        long count = spark.sql("SELECT * FROM points_lilis WHERE x >= -87.70 AND x <= -87.64 AND y >= 41.82 AND y <= 41.86").count();
        assert count == 3 : "Expected 3 indexed SQL rows, got " + count;
        System.out.println("PASSED");
    }

    private static void testManualDataSourceView(SparkSession spark) {
        System.out.print("[2] CREATE TEMPORARY VIEW USING lilis... ");
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW indexed_points USING lilis OPTIONS (index_name 'points')");
        long count = spark.sql("SELECT * FROM indexed_points WHERE x > -87.70 AND x < -87.64 AND y > 41.82 AND y < 41.86").count();
        assert count == 3 : "Expected 3 indexed SQL rows, got " + count;
        System.out.println("PASSED");
    }

    private static void testProgrammaticQueryView(SparkSession spark) {
        System.out.print("[3] indexed range query as temp view... ");
        LiLIS.rangeQueryAsView(spark, "points", "range_result", -87.70, 41.82, -87.64, 41.86);
        long count = spark.sql("SELECT * FROM range_result").count();
        assert count == 3 : "Expected 3 range view rows, got " + count;
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
}

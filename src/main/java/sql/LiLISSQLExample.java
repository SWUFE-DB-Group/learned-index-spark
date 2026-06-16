package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LiLISSQLExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("LiLIS-SQL-Example")
                .master(args.length > 2 ? args[2] : "local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        try {
            LiLIS.register(spark);

            Dataset<Row> points;
            if (args.length > 0) {
                points = LiLIS.loadCSV(spark, args[0]);
            } else {
                points = spark.createDataFrame(java.util.Arrays.asList(
                        new PointRow(-87.697249, 41.822730),
                        new PointRow(-87.686513, 41.830143),
                        new PointRow(-87.650000, 41.850000),
                        new PointRow(-87.620000, 41.870000)
                ), PointRow.class).select("x", "y");
            }
            points.createOrReplaceTempView("points");

            IndexedSpatialRDD index = LiLIS.createIndex(spark, points, args.length > 1 ? args[1] : "QuadTree");

            System.out.println("=== Indexed range query ===");
            LiLIS.rangeQuery(spark, index, -87.70, 41.82, -87.64, 41.86).show(false);

            System.out.println("=== SQL UDF range filter (row-level) ===");
            spark.sql("SELECT * FROM points WHERE lilis_in_range(x, y, -87.70, 41.82, -87.64, 41.86)").show(false);

            System.out.println("=== Indexed KNN query ===");
            LiLIS.knnQuery(spark, index, -87.65, 41.85, 2).show(false);
        } finally {
            spark.stop();
        }
    }

    public static class PointRow implements java.io.Serializable {
        private double x;
        private double y;

        public PointRow() {
        }

        public PointRow(double x, double y) {
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

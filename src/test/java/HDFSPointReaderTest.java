import datatypes.Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.HDFSPointReader;

import java.util.List;

public class HDFSPointReaderTest {
    public static void main(String[] args) {
        System.out.println("=== HDFSPointReader Test ===\n");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("HDFSPointReaderTest")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.host", "localhost");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        try {
            testReadCSVWithHeader(sc);
            testReadCSVNoHeader(sc);
            testReadCSVCustomOffset(sc);
            testReadCSVCustomDelimiter(sc);
            System.out.println("\n=== ALL HDFSPointReader TESTS PASSED ===");
        } finally {
            sc.close();
        }
    }

    static void testReadCSVWithHeader(JavaSparkContext sc) {
        System.out.print("[1] readCSV (有表头, 默认列)... ");
        JavaRDD<Point> rdd = HDFSPointReader.readCSV(sc, "/tmp/lilis-test/points.csv");
        List<Point> points = rdd.collect();
        assert points.size() == 5 : "Expected 5, got " + points.size();
        assert Math.abs(points.get(0).getX() - (-87.697249)) < 0.0001;
        assert Math.abs(points.get(0).getY() - 41.822730) < 0.0001;
        System.out.println("PASSED (" + points.size() + " points)");
    }

    static void testReadCSVNoHeader(JavaSparkContext sc) {
        System.out.print("[2] readCSVNoHeader (无表头)... ");
        // 创建无表头测试文件使用 points_noheader.csv
        JavaRDD<Point> rdd = HDFSPointReader.readCSVNoHeader(sc, "/tmp/lilis-test/points_noheader.csv");
        List<Point> points = rdd.collect();
        assert points.size() == 5 : "Expected 5, got " + points.size();
        assert Math.abs(points.get(0).getX() - (-87.697249)) < 0.0001;
        System.out.println("PASSED (" + points.size() + " points)");
    }

    static void testReadCSVCustomOffset(JavaSparkContext sc) {
        System.out.print("[3] readCSV (自定义列偏移 x=1, y=2)... ");
        // multi_col.csv: id, x, y
        JavaRDD<Point> rdd = HDFSPointReader.readCSV(sc, "/tmp/lilis-test/multi_col.csv", 1, 2, true);
        List<Point> points = rdd.collect();
        assert points.size() == 3 : "Expected 3, got " + points.size();
        assert Math.abs(points.get(0).getX() - 1.5) < 0.0001;
        assert Math.abs(points.get(0).getY() - 2.5) < 0.0001;
        System.out.println("PASSED (" + points.size() + " points)");
    }

    static void testReadCSVCustomDelimiter(JavaSparkContext sc) {
        System.out.print("[4] readCSV (自定义分隔符 TAB)... ");
        JavaRDD<Point> rdd = HDFSPointReader.readCSV(sc, "/tmp/lilis-test/points.tsv", 0, 1, true, "\t");
        List<Point> points = rdd.collect();
        assert points.size() == 3 : "Expected 3, got " + points.size();
        assert Math.abs(points.get(0).getX() - 10.0) < 0.0001;
        System.out.println("PASSED (" + points.size() + " points)");
    }
}

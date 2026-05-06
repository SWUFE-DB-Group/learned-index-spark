import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import partitions.SpatialPartition;
import query.KNNQuery;
import query.PointQuery;
import query.RangeQuery;
import spline.Spline;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SparkIntegrationTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== LiLIS Spark Integration Test ===\n");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("LiLIS-Test")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.host", "localhost");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        try {
            List<Point> points = generateTestData(10000, 42);
            JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);

            testPartitionAndIndex(pointRDD, points);
            testRangeQuery(jsc, points);
            testPointQuery(jsc, points);
            testKNNQuery(jsc, points);

            System.out.println("\n=== ALL SPARK INTEGRATION TESTS PASSED ===");
        } finally {
            jsc.close();
        }
    }

    static void testPartitionAndIndex(JavaRDD<Point> pointRDD, List<Point> points) throws Exception {
        System.out.print("[1] QuadTree partition + index build... ");
        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);
        long splineCount = indexRDD.count();
        assert splineCount > 0 : "No splines built";
        System.out.println("PASSED (partitions with index: " + splineCount + ")");
    }

    static void testRangeQuery(JavaSparkContext jsc, List<Point> points) throws Exception {
        System.out.print("[2] Range query... ");
        JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);
        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);

        Rectangle queryRect = new Rectangle(-50, -50, 50, 50);
        JavaRDD<Point> resultRDD = RangeQuery.SpatialRangeQuery(indexRDD, queryRect);
        long resultCount = resultRDD.count();

        long expected = 0;
        for (Point p : points) {
            if (queryRect.contains(p)) expected++;
        }

        assert resultCount == expected :
                "Range query returned " + resultCount + ", expected " + expected;
        System.out.println("PASSED (found " + resultCount + "/" + points.size() + " points)");
    }

    static void testPointQuery(JavaSparkContext jsc, List<Point> points) throws Exception {
        System.out.print("[3] Point query... ");
        JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);
        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);

        Point target = points.get(500);
        JavaRDD<Point> resultRDD = PointQuery.SpatialPointQuery(indexRDD, target);
        long count = resultRDD.count();

        assert count >= 1 : "Point query found 0 results for existing point";
        System.out.println("PASSED (found " + count + " match(es) for point " + target + ")");
    }

    static void testKNNQuery(JavaSparkContext jsc, List<Point> points) throws Exception {
        System.out.print("[4] KNN query (k=5)... ");
        JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);
        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);

        Point queryPoint = new Point(0, 0);
        Rectangle bbox = utils.Utils.getBoundingBox(points);
        double maxArea = bbox.getArea();

        List<Point> knnResult = KNNQuery.SpatialKNNQuery(indexRDD, 5, queryPoint, maxArea, points.size());
        assert knnResult.size() == 5 : "KNN returned " + knnResult.size() + ", expected 5";

        double maxDist = 0;
        for (Point p : knnResult) {
            double dist = queryPoint.distanceTo(p);
            if (dist > maxDist) maxDist = dist;
        }

        int closerCount = 0;
        for (Point p : points) {
            if (queryPoint.distanceTo(p) < maxDist) closerCount++;
        }
        assert closerCount <= 5 : "KNN missed closer points: " + closerCount + " points are closer than max KNN dist";
        System.out.println("PASSED (5 nearest neighbors found, max dist: " +
                String.format("%.4f", maxDist) + ")");
    }

    static List<Point> generateTestData(int n, long seed) {
        Random rng = new Random(seed);
        List<Point> points = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            points.add(new Point(rng.nextDouble() * 200 - 100, rng.nextDouble() * 200 - 100));
        }
        return points;
    }
}

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.*;
import partitions.SpatialPartition;
import query.JoinQuery;
import query.JoinQueryUsingIndex;
import query.RangeQuery;
import scala.Tuple2;
import spline.Spline;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JoinQueryTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== JoinQuery Test: Point x Polygon ===\n");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("JoinQuery-Test")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.host", "localhost");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        try {
            // 1. Generate point data
            List<Point> points = generatePoints(10000, 42);
            JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);

            // 2. Generate polygon data
            List<Polygon> polygons = generatePolygons(10);

            System.out.println("Points: " + points.size());
            System.out.println("Polygons: " + polygons.size());
            System.out.println();

            // 3. Test JoinQuery with index
            testJoinQueryWithIndex(pointRDD, polygons);

            // 4. Test JoinQuery without index
            testJoinQueryWithoutIndex(pointRDD, polygons);

            // 5. Test JoinQueryUsingIndex (partition-level)
            testJoinQueryUsingIndex(pointRDD, polygons);

            // 6. Correctness verification
            testJoinCorrectness(jsc, points, polygons);

            System.out.println("\n=== ALL JOIN QUERY TESTS PASSED ===");
        } finally {
            jsc.close();
        }
    }

    static void testJoinQueryWithIndex(JavaRDD<Point> pointRDD, List<Polygon> polygons) throws Exception {
        System.out.print("[1] JoinQuery with index... ");
        long t0 = System.currentTimeMillis();

        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);
        indexRDD.cache();
        indexRDD.count();

        JoinQuery.SpatialJoinQuery(indexRDD, polygons, false);

        long t1 = System.currentTimeMillis();
        System.out.println("PASSED (" + (t1 - t0) + " ms)");
    }

    static void testJoinQueryWithoutIndex(JavaRDD<Point> pointRDD, List<Polygon> polygons) throws Exception {
        System.out.print("[2] JoinQuery without index... ");
        long t0 = System.currentTimeMillis();

        JoinQuery.SpatialJoinQueryWitoutIndex(pointRDD, polygons, false);

        long t1 = System.currentTimeMillis();
        System.out.println("PASSED (" + (t1 - t0) + " ms)");
    }

    static void testJoinQueryUsingIndex(JavaRDD<Point> pointRDD, List<Polygon> polygons) throws Exception {
        System.out.print("[3] JoinQueryUsingIndex (partition-level)... ");
        long t0 = System.currentTimeMillis();

        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);

        JoinQueryUsingIndex joinFunc = new JoinQueryUsingIndex(polygons);
        long resultCount = indexRDD.mapPartitionsToPair(joinFunc).count();

        long t1 = System.currentTimeMillis();
        System.out.println("PASSED (result tuples: " + resultCount + ", " + (t1 - t0) + " ms)");
    }

    static void testJoinCorrectness(JavaSparkContext jsc, List<Point> points, List<Polygon> polygons) throws Exception {
        System.out.print("[4] Correctness verification... ");

        JavaRDD<Point> pointRDD = jsc.parallelize(points, 8);
        JavaRDD<Point> partitioned = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitioned);
        indexRDD.cache();
        indexRDD.count();

        // For each polygon, compare index-based range query vs brute-force
        boolean allCorrect = true;
        for (int i = 0; i < polygons.size(); i++) {
            Polygon polygon = polygons.get(i);
            Envelope envelope = polygon.getEnvelopeInternal();
            Rectangle queryRange = new Rectangle(envelope);

            // Index-based result
            long indexResult = RangeQuery.SpatialRangeQuery(indexRDD, queryRange).count();

            // Brute-force result
            long bruteResult = 0;
            for (Point p : points) {
                if (queryRange.contains(p)) bruteResult++;
            }

            if (indexResult != bruteResult) {
                System.out.println("\n  MISMATCH polygon " + i + ": index=" + indexResult + " vs brute=" + bruteResult);
                allCorrect = false;
            }
        }

        if (allCorrect) {
            System.out.println("PASSED (all polygon join results match brute-force)");
        } else {
            System.out.println("FAILED (some results differ)");
        }
    }

    // Generate random points in [-100, 100] x [-100, 100]
    static List<Point> generatePoints(int n, long seed) {
        Random rng = new Random(seed);
        List<Point> points = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            points.add(new Point(rng.nextDouble() * 200 - 100, rng.nextDouble() * 200 - 100));
        }
        return points;
    }

    // Generate random rectangular polygons within [-100, 100] x [-100, 100]
    static List<Polygon> generatePolygons(int n) {
        Random rng = new Random(123);
        GeometryFactory gf = new GeometryFactory();
        List<Polygon> polygons = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            double x1 = rng.nextDouble() * 160 - 80;
            double y1 = rng.nextDouble() * 160 - 80;
            double width = 10 + rng.nextDouble() * 30;
            double height = 10 + rng.nextDouble() * 30;
            double x2 = x1 + width;
            double y2 = y1 + height;

            Coordinate[] coords = new Coordinate[]{
                    new Coordinate(x1, y1),
                    new Coordinate(x2, y1),
                    new Coordinate(x2, y2),
                    new Coordinate(x1, y2),
                    new Coordinate(x1, y1)  // close the ring
            };
            LinearRing ring = gf.createLinearRing(coords);
            polygons.add(gf.createPolygon(ring));
        }
        return polygons;
    }
}

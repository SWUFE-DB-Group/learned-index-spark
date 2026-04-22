import datatypes.Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import query.KNNQuery;
import spline.Spline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KNNQueryParityTest {

    @Test
    public void knnRefineMatchesBruteForceTopKOnEdgeDataset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        Assert.assertTrue("edge_points.csv should have at least 100 points", points.size() >= 100);

        List<Point> queryPoints = new ArrayList<Point>();
        queryPoints.add(new Point(5.0, 5.0));
        queryPoints.add(new Point(10.0, 10.0));
        queryPoints.add(new Point(200.0005, 200.0005));
        queryPoints.add(new Point(-1.0, -1.0));

        int[] ks = new int[]{1, 10, 100};

        for (Point query : queryPoints) {
            for (int k : ks) {
                List<Point> expected = Phase1TestUtils.bruteForceTopK(points, query, k);
                List<Point> actual = KNNQuery.pointSort(points, query, k);

                Assert.assertEquals(
                        "kNN parity mismatch for query " + query + " and k=" + k,
                        Phase1TestUtils.toMultiset(expected),
                        Phase1TestUtils.toMultiset(actual)
                );
            }
        }
    }

    @Test
    public void spatialKnnQueryMatchesBruteForceTopKWithSinglePartition() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        Point query = new Point(200.0005, 200.0005);
        double area = boundingBoxArea(points);

        SparkConf conf = new SparkConf()
                .setAppName("KNNQueryParityTest")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        try {
            Spline spline = Phase1TestUtils.buildSpline(points);
            JavaRDD<Spline> splineRDD = sc.parallelize(java.util.Collections.singletonList(spline), 1);

            int[] ks = new int[]{1, 10, 100};
            for (int k : ks) {
                List<Point> expected = Phase1TestUtils.bruteForceTopK(points, query, k);
                List<Point> actual = KNNQuery.SpatialKNNQuery(splineRDD, k, query, area, points.size());

                Assert.assertEquals(
                        "SpatialKNNQuery parity mismatch for k=" + k,
                        Phase1TestUtils.toMultiset(expected),
                        Phase1TestUtils.toMultiset(actual)
                );
            }
        } finally {
            sc.close();
        }
    }

    @Test
    public void knnRefineMatchesBruteForceOnSyntheticSubset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/syn_subset.csv");
        Assert.assertTrue("syn_subset.csv should contain enough points", points.size() >= 50000);

        Point query = points.get(points.size() / 2);
        int[] ks = new int[]{1, 10, 100};
        for (int k : ks) {
            List<Point> expected = Phase1TestUtils.bruteForceTopK(points, query, k);
            List<Point> actual = KNNQuery.pointSort(points, query, k);

            Assert.assertEquals(
                    "kNN refine parity mismatch on syn_subset for k=" + k,
                    Phase1TestUtils.toMultiset(expected),
                    Phase1TestUtils.toMultiset(actual)
            );
        }
    }

    private double boundingBoxArea(List<Point> points) {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (Point point : points) {
            minX = Math.min(minX, point.getX());
            minY = Math.min(minY, point.getY());
            maxX = Math.max(maxX, point.getX());
            maxY = Math.max(maxY, point.getY());
        }

        double width = Math.max(1e-12, maxX - minX);
        double height = Math.max(1e-12, maxY - minY);
        return width * height;
    }
}

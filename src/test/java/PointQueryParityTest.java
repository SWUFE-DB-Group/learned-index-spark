import datatypes.Point;
import org.junit.Assert;
import org.junit.Test;
import spline.Spline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PointQueryParityTest {

    @Test
    public void pointLookupMatchesBruteForceOnEdgeDataset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        Assert.assertTrue("edge_points.csv should have >100 rows to trigger learned build path", points.size() > 100);

        Spline spline = Phase1TestUtils.buildSpline(points);

        List<Point> queries = new ArrayList<Point>();

        int existingCount = Math.min(50, points.size());
        int step = Math.max(1, points.size() / existingCount);
        for (int i = 0; i < points.size() && queries.size() < existingCount; i += step) {
            queries.add(points.get(i));
        }

        int missingToAdd = 100 - queries.size();
        for (int i = 0; i < missingToAdd; i++) {
            Point base = points.get(i % points.size());
            Point candidate = new Point(base.getX() + 12345.678 + i, base.getY() - 9876.543 - i);
            queries.add(candidate);
        }

        for (Point query : queries) {
            boolean expected = Phase1TestUtils.bruteForceContains(points, query);
            boolean actual = spline.pointLookUp(query);
            Assert.assertEquals("Point parity mismatch for query " + query, expected, actual);
        }
    }

    @Test
    public void pointLookupMatchesBruteForceOnSmokeDataset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/smoke_points.csv");
        Assert.assertTrue("smoke_points.csv should contain enough points", points.size() >= 1000);

        List<Point> sample = new ArrayList<Point>();
        int stride = Math.max(1, points.size() / 100);
        for (int i = 0; i < points.size() && sample.size() < 100; i += stride) {
            sample.add(points.get(i));
        }

        Spline spline = Phase1TestUtils.buildSpline(sample);
        Assert.assertFalse("Smoke sample with 100 points should stay in fallback linear mode", spline.isBuild());
        Assert.assertTrue("Smoke sample with 100 points should use linear scan", spline.isLinear_scan());

        runPointParityChecks(sample, spline);
    }

    @Test
    public void pointLookupFallbackMatchesBruteForceOnAtMostHundredPoints() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        List<Point> fallbackPoints = new ArrayList<Point>(points.subList(0, 100));

        Spline spline = Phase1TestUtils.buildSpline(fallbackPoints);
        Assert.assertFalse("<=100 points should not build learned index", spline.isBuild());
        Assert.assertTrue("<=100 points should use fallback linear scan", spline.isLinear_scan());

        runPointParityChecks(fallbackPoints, spline);
    }

    private void runPointParityChecks(List<Point> points, Spline spline) {
        List<Point> queries = new ArrayList<Point>();

        int existingCount = Math.min(50, points.size());
        int step = Math.max(1, points.size() / existingCount);
        for (int i = 0; i < points.size() && queries.size() < existingCount; i += step) {
            queries.add(points.get(i));
        }

        int missingToAdd = 100 - queries.size();
        for (int i = 0; i < missingToAdd; i++) {
            Point base = points.get(i % points.size());
            Point candidate = new Point(base.getX() + 12345.678 + i, base.getY() - 9876.543 - i);
            queries.add(candidate);
        }

        for (Point query : queries) {
            boolean expected = Phase1TestUtils.bruteForceContains(points, query);
            boolean actual = spline.pointLookUp(query);
            Assert.assertEquals("Point parity mismatch for query " + query, expected, actual);
        }
    }
}

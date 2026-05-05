import datatypes.Point;
import datatypes.Rectangle;
import org.junit.Assert;
import org.junit.Test;
import spline.Spline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RangeQueryParityTest {

    @Test
    public void rangeLookupMatchesBruteForceOnEdgeDataset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        Assert.assertTrue("edge_points.csv should have >100 rows to trigger learned build path", points.size() > 100);

        Spline spline = Phase1TestUtils.buildSpline(points);

        runRangeParityChecks(points, spline, 100, 20260422L);
    }

    @Test
    public void rangeLookupMatchesBruteForceOnSyntheticSubset() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/syn_subset.csv");
        Assert.assertTrue("syn_subset.csv should contain enough points", points.size() >= 50000);

        Spline spline = Phase1TestUtils.buildSpline(points);
        Assert.assertTrue("syn_subset.csv should trigger learned build path", spline.isBuild());
        Assert.assertFalse("syn_subset.csv should not use fallback path", spline.isLinear_scan());

        runRangeParityChecks(points, spline, 40, 20260423L);
    }

    @Test
    public void rangeLookupFallbackMatchesBruteForceOnAtMostHundredPoints() throws IOException {
        List<Point> points = Phase1TestUtils.readPointsFromCsv("data/edge_points.csv");
        List<Point> fallbackPoints = new ArrayList<Point>(points.subList(0, 100));

        Spline spline = Phase1TestUtils.buildSpline(fallbackPoints);
        Assert.assertFalse("<=100 points should not build learned index", spline.isBuild());
        Assert.assertTrue("<=100 points should use fallback linear scan", spline.isLinear_scan());

        runRangeParityChecks(fallbackPoints, spline, 50, 20260424L);
    }

    private void runRangeParityChecks(List<Point> points, Spline spline, int rectangleCount, long seed) {

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

        List<Rectangle> rectangles = new ArrayList<Rectangle>();

        rectangles.add(new Rectangle(minX - 1000, minY - 1000, minX - 500, minY - 500));
        rectangles.add(new Rectangle(minX, minY, maxX, maxY));
        rectangles.add(new Rectangle(minX, minY, minX, minY));
        rectangles.add(new Rectangle(maxX, maxY, maxX, maxY));
        rectangles.add(new Rectangle(minX, minY, minX + 1.0, minY + 1.0));
        rectangles.add(new Rectangle(maxX - 1.0, maxY - 1.0, maxX, maxY));

        Random random = new Random(seed);
        while (rectangles.size() < rectangleCount) {
            double x1 = minX - 5 + random.nextDouble() * (maxX - minX + 10);
            double x2 = minX - 5 + random.nextDouble() * (maxX - minX + 10);
            double y1 = minY - 5 + random.nextDouble() * (maxY - minY + 10);
            double y2 = minY - 5 + random.nextDouble() * (maxY - minY + 10);

            double xl = Math.min(x1, x2);
            double xh = Math.max(x1, x2);
            double yl = Math.min(y1, y2);
            double yh = Math.max(y1, y2);
            rectangles.add(new Rectangle(xl, yl, xh, yh));
        }

        for (Rectangle rectangle : rectangles) {
            List<Point> expected = Phase1TestUtils.bruteForceRange(points, rectangle);
            List<Point> actual = new ArrayList<Point>();
            spline.lookUp(rectangle, actual);

            Assert.assertEquals(
                    "Range parity mismatch for rectangle " + rectangle,
                    Phase1TestUtils.toMultiset(expected),
                    Phase1TestUtils.toMultiset(actual)
            );
        }
    }
}

import datatypes.Point;
import org.junit.Assert;
import org.junit.Test;
import spline.Spline;

import java.util.ArrayList;
import java.util.List;

public class SplineBuildTest {

    @Test
    public void fallbackModeWhenPartitionIsAtMostThreshold() {
        List<Point> points = makePoints(100);
        Spline spline = Phase1TestUtils.buildSpline(points);

        Assert.assertTrue("Partition <=100 should use linear scan", spline.isLinear_scan());
        Assert.assertFalse("Partition <=100 does not require learned index build", spline.isBuild());
        Assert.assertTrue("Fallback mode should not need spline points", spline.getSpline().isEmpty());
        Assert.assertNull("Fallback mode should not require radix hints", spline.getRadixHint());
    }

    @Test
    public void learnedIndexModeWhenPartitionExceedsThreshold() {
        List<Point> points = makePoints(150);
        Spline spline = Phase1TestUtils.buildSpline(points);

        Assert.assertTrue("Partition >100 should build learned index", spline.isBuild());
        Assert.assertFalse("Partition >100 should not stay in linear scan mode", spline.isLinear_scan());
        Assert.assertNotNull("Spline should be built", spline.getSpline());
        Assert.assertFalse("Spline should contain segments", spline.getSpline().isEmpty());
        Assert.assertNotNull("Radix hints should be built", spline.getRadixHint());

        List<Point> sorted = spline.getPoints();
        for (int i = 1; i < sorted.size(); i++) {
            Assert.assertTrue(
                    "Points should be sorted by Y after build",
                    sorted.get(i - 1).getY() <= sorted.get(i).getY()
            );
        }
    }

    @Test
    public void constantsMatchContract() {
        Assert.assertEquals(32, Spline.getSplineSize());
        Assert.assertEquals(10, Spline.getRadixSize());
    }

    private List<Point> makePoints(int size) {
        List<Point> points = new ArrayList<Point>();
        for (int i = 0; i < size; i++) {
            double x = (i % 17) * 0.7;
            double y = (size - i) * 0.13;
            points.add(new Point(x, y));
        }
        return points;
    }
}

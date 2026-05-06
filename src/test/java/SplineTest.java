import datatypes.Point;
import datatypes.Rectangle;
import spline.Spline;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SplineTest {
    public static void main(String[] args) {
        System.out.println("=== LiLIS Core Unit Test ===\n");

        testBasicDataTypes();
        testSplineBuildSmall();
        testSplineBuildLarge();
        testPointLookup();
        testRangeLookup();
        testLinearScanFallback();

        System.out.println("\n=== ALL TESTS PASSED ===");
    }

    static void testBasicDataTypes() {
        System.out.print("[1] Basic datatypes... ");
        Point p1 = new Point(1.0, 2.0);
        Point p2 = new Point(3.0, 4.0);
        assert p1.getX() == 1.0 && p1.getY() == 2.0;
        assert p1.euclideanDistance(p2) == Math.sqrt(8.0);

        Rectangle rect = new Rectangle(1.0, 2.0, 3.0, 4.0);
        assert rect.contains(new Point(2.0, 3.0));
        assert !rect.contains(new Point(5.0, 5.0));
        assert rect.getArea() == 4.0;

        Rectangle rect2 = new Rectangle(2.5, 3.5, 5.0, 6.0);
        assert rect.intersects(rect2);

        List<Point> pts = new ArrayList<>();
        pts.add(p1);
        pts.add(p2);
        Rectangle bbox = Utils.getBoundingBox(pts);
        assert bbox.getFrom().getX() == 1.0;
        assert bbox.getTo().getX() == 3.0;

        System.out.println("PASSED");
    }

    static void testSplineBuildSmall() {
        System.out.print("[2] Spline build (small, linear scan fallback)... ");
        Spline spline = new Spline();
        for (int i = 0; i < 50; i++) {
            spline.add(new Point(i * 0.1, i * 0.2));
        }
        spline.build();
        assert spline.isLinear_scan();
        assert !spline.isBuild();
        assert spline.size() == 50;
        System.out.println("PASSED");
    }

    static void testSplineBuildLarge() {
        System.out.print("[3] Spline build (large, full index)... ");
        Spline spline = buildLargeSpline(1000);
        assert spline.isBuild();
        assert !spline.isLinear_scan();
        assert spline.size() == 1000;
        assert spline.getSpline() != null && spline.getSpline().size() > 0;
        assert spline.getRadixHint() != null;
        System.out.println("PASSED (spline points: " + spline.getSpline().size() + ")");
    }

    static void testPointLookup() {
        System.out.print("[4] Point lookup... ");
        Spline spline = new Spline();
        Random rng = new Random(42);
        List<Point> allPoints = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            Point p = new Point(rng.nextDouble() * 100, rng.nextDouble() * 100);
            spline.add(p);
            allPoints.add(p);
        }
        spline.build();

        int foundCount = 0;
        for (int i = 0; i < 20; i++) {
            Point target = allPoints.get(i * 25);
            boolean found = spline.pointLookUp(target);
            if (found) foundCount++;
        }
        assert foundCount == 20 : "Expected 20, got " + foundCount;
        System.out.println("PASSED (" + foundCount + "/20 found)");
    }

    static void testRangeLookup() {
        System.out.print("[5] Range lookup... ");
        Spline spline = new Spline();
        Random rng = new Random(123);
        int totalPoints = 2000;
        for (int i = 0; i < totalPoints; i++) {
            spline.add(new Point(rng.nextDouble() * 100, rng.nextDouble() * 100));
        }
        spline.build();

        Rectangle queryRect = new Rectangle(20, 20, 80, 80);
        List<Point> result = new ArrayList<>();
        spline.lookUp(queryRect, result);

        int bruteForceCount = 0;
        for (Point p : spline.getPoints()) {
            if (queryRect.contains(p)) bruteForceCount++;
        }

        assert result.size() == bruteForceCount :
                "Spline returned " + result.size() + " but brute force found " + bruteForceCount;
        System.out.println("PASSED (found " + result.size() + "/" + totalPoints + " points in range)");
    }

    static void testLinearScanFallback() {
        System.out.print("[6] Linear scan fallback for small partitions... ");
        Spline spline = new Spline();
        for (int i = 0; i < 80; i++) {
            spline.add(new Point(i, i * 2));
        }
        spline.build();
        assert spline.isLinear_scan();

        Rectangle queryRect = new Rectangle(10, 20, 50, 100);
        List<Point> result = new ArrayList<>();
        spline.lookUp(queryRect, result);

        int expected = 0;
        for (int i = 0; i < 80; i++) {
            if (i >= 10 && i <= 50 && i * 2 >= 20 && i * 2 <= 100) expected++;
        }
        assert result.size() == expected :
                "Expected " + expected + ", got " + result.size();
        System.out.println("PASSED (" + result.size() + " results)");
    }

    static Spline buildLargeSpline(int n) {
        Spline spline = new Spline();
        Random rng = new Random(0);
        for (int i = 0; i < n; i++) {
            spline.add(new Point(rng.nextDouble() * 180 - 90, rng.nextDouble() * 360 - 180));
        }
        spline.build();
        return spline;
    }
}

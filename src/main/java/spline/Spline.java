package spline;

import datatypes.Circle;
import datatypes.Point;
import datatypes.Rectangle;
import utils.SplineUtil;
import utils.Utils;

import java.io.Serializable;
import java.util.*;


public class Spline implements Serializable {

    private static final int FALLBACK_TO_LINEAR_SCAN_THRESHOLD = 100;



    public static final int SPLINE_SIZE = 32;
    public static final int RADIX_SIZE = 10;

    private boolean isBuild = false;
    private boolean linear_scan = false;
    private List<Point> points;
    private List<Coord> spline;
    int[] radixHint;
    //private List<Integer> radixHint;
    private int n;
    private double min_y, max_y, min_x, max_x;

    private double factor;


    public Spline() {
        this.points = new ArrayList<>();
        this.spline = new ArrayList<>();
    }


    public void build() {
        this.min_x = Double.MAX_VALUE;
        this.min_y = Double.MAX_VALUE;
        this.max_x = -Double.MAX_VALUE;
        this.max_y = -Double.MAX_VALUE;

        for (Point point : points) {
            if (min_x > point.getX()) {
                min_x = point.getX();
            }
            if (min_y > point.getY()) {
                min_y = point.getY();
            }
            if (max_x < point.getX()) {
                max_x = point.getX();
            }
            if (max_y < point.getY()) {
                max_y = point.getY();
            }
        }

        if (points.size() <= FALLBACK_TO_LINEAR_SCAN_THRESHOLD) {
            linear_scan = true;
            return;
        }
        Utils.sortPointsY(points);
        CdfOnTheFlyInterfaceY cdf = new CdfOnTheFlyInterfaceY(points);
        spline = SplineUtil.tautString(cdf,SPLINE_SIZE);
        buildRadix(RADIX_SIZE);
        isBuild = true;
        //System.out.println("Building....");
    }


    public static int getFallbackToLinearScanThreshold() {
        return FALLBACK_TO_LINEAR_SCAN_THRESHOLD;
    }

    public double getMin_y() {
        return min_y;
    }

    public double getMax_y() {
        return max_y;
    }

    public double getMin_x() {
        return min_x;
    }

    public double getMax_x() {
        return max_x;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }

    private long transform(double val) {
        return (long) ((val - min_y) * factor);
    }



    public void buildRadix(int numRadixBits) {
        //System.out.println("buildRadix ing....");
        if (numRadixBits <= 0) {
            throw new IllegalArgumentException("numRadixBits must be greater than 0.");
        }

        // Allocate memory for the hints
       // radixHint = new ArrayList<Integer>((int) (1L << numRadixBits) + 2);
        radixHint = new int[(int) (1L << numRadixBits) + 2];
        // Compute the number of bits to shift with
        n = spline.size();
        min_y = spline.get(0).getX();
        max_y = spline.get(n - 1).getX();

        if (min_y != max_y) {
            factor = (1L << numRadixBits) / (max_y - min_y);
        } else {
            factor = 0;
        }

        // Compute the hints
        //radixHint.add(0);
        radixHint[0]=0;
        long prevPrefix = 0;

        for (int i = 0; i < n; i++) {
            long currPrefix = transform(spline.get(i).getX());

            /*if (currPrefix >= radixHint.size()) {
                //throw new IllegalArgumentException("Current prefix is out of bounds.");
                continue;
            }*/
            if (currPrefix >= radixHint.length) {
                //throw new IllegalArgumentException("Current prefix is out of bounds.");
                continue;
            }

            if (currPrefix != prevPrefix) {
                for (long j = prevPrefix + 1; j <= currPrefix; j++) {
                    if (j >= radixHint.length) {
                        //throw new IllegalArgumentException("Hint index is out of bounds.");
                        continue;
                    }
                    //radixHint.add((int) j, i);
                    radixHint[(int) j]=i;
                }

                prevPrefix = currPrefix;
            }
        }

        // Margin hint values
        for (; prevPrefix < radixHint.length - 1; prevPrefix++) {
            if (prevPrefix + 1 < radixHint.length) {
               // radixHint.add((int) (prevPrefix + 1), n - 1);
                radixHint[(int) (prevPrefix + 1)] = n-1;
            }
        }
    }

    public int count(Rectangle rectangle) {
        if (!isBuild) {
            // Handle the case when not built
            //return 0;
        }

        // Check if the rectangle intersects with the bounding box
        if (rectangle.getFrom().getY() > max_y || rectangle.getTo().getY() < min_y) {
            return 0;
        }

        if (rectangle.getFrom().getX() <= min_x && max_x <= rectangle.getTo().getX()) {
            if (rectangle.getFrom().getY() <= min_y && max_y <= rectangle.getTo().getY()) {
                return points.size();
            }
            if (linear_scan) {
                return linearScanCount(rectangle);
            }
            return countContained(rectangle);
        }

        if (linear_scan) {
            return linearScanCount(rectangle);
        }

        // Handle other cases
        if (rectangle.getFrom().getY() <= min_y) {
            return countUnderShot(rectangle, 0);
        }
        if (rectangle.getTo().getY() >= max_y) {
            return countOverShot(rectangle, points.size() - 1);
        }

        long estimate = estimatePosition((rectangle.getFrom().getY() + rectangle.getTo().getY()) / 2);

        if (points.get((int) estimate).getY() > rectangle.getTo().getY()) {
            return countOverShot(rectangle, (int) estimate);
        } else if (points.get((int) estimate).getY() < rectangle.getFrom().getY()) {
            return countUnderShot(rectangle, (int) estimate);
        } else {
            return countOverShot(rectangle, (int) estimate) + countUnderShot(rectangle, (int) (estimate + 1));
        }
    }

    public int indexLookup(Rectangle rectangle) {
        int count = 0;

        if (!isBuild) {
            // Handle the case when not built
            return points.size();
        }

        // Check if the rectangle intersects with the bounding box
        if (rectangle.getFrom().getY() > max_y || rectangle.getTo().getY() < min_y) {
            return points.size();
        }

        if (rectangle.getFrom().getX() <= min_x && max_x <= rectangle.getTo().getX()) {
            if (rectangle.getFrom().getY() <= min_y && max_y <= rectangle.getTo().getY()) {
                return points.size();
            }
            if (linear_scan) {
                return points.size();
            }
            long from = estimateFrom(rectangle);
            long to = estimateTo(rectangle);
            return points.size();
        }

        if (linear_scan) {
            return points.size();
        }

        if (rectangle.getFrom().getY() <= min_y) {
            int estimate = 0;
            while (points.get(estimate).getY() < rectangle.getFrom().getY() && estimate < points.size()) {
                estimate++;
            }
            return points.size();
        }

        if (rectangle.getTo().getY() >= max_y) {
            int estimate = points.size() - 1;
            while (points.get(estimate).getY() > rectangle.getTo().getY() && estimate > 0) {
                estimate--;
            }
            return points.size();
        }

        long estimate = estimatePosition((rectangle.getFrom().getY() + rectangle.getTo().getY()) / 2);

        if (points.get((int) estimate).getY() > rectangle.getTo().getY()) {
            while (points.get((int) estimate).getY() > rectangle.getTo().getY() && estimate > 0) {
                estimate--;
            }
            return points.size();
        } else if (points.get((int) estimate).getY() < rectangle.getFrom().getY()) {
            while (points.get((int) estimate).getY() < rectangle.getFrom().getY() && estimate < points.size()) {
                estimate++;
            }
            return points.size();
        }

        long estimateOver = estimate;
        long estimateUnder = estimate + 1;

        while (points.get((int) estimateOver).getY() > rectangle.getTo().getY() && estimateOver > 0) {
            estimateOver--;
        }

        while (points.get((int) estimateUnder).getY() < rectangle.getFrom().getY() && estimateUnder < points.size()) {
            estimateUnder++;
        }

        return points.size();
    }

    public int count(Circle circle) {
        throw new UnsupportedOperationException("Method not implemented");
    }
    public long size() { return points.size(); }

    public void pointLookUp(Point queryPoint, List<Point> result) {
        if (linear_scan) {
            for (Point point : points) {
                if (point.equals(queryPoint)) {
                    result.add(point);
                    return;
                }
            }
            return;
        }
        if (!isBuild)
           // System.out.println("no BuildSpline");
            throw new AssertionError();

        // Estimate the from and to position using the spline
        long estimate = 0;
        if (queryPoint.getY() > min_y) {
            estimate = estimatePosition(queryPoint.getY());
        }

        // If we overshoot
        if (points.get((int) estimate).getY() > queryPoint.getY()) {
            while (points.get((int) estimate).getY() > queryPoint.getY() && estimate > 0) {
                estimate--;
            }
            while (estimate >= 0 && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    result.add(points.get((int) estimate));
                    return;
                }
                estimate--;
            }
        }
        // If RS underestimates
        else if (points.get((int) estimate).getY() < queryPoint.getY()) {
            while (points.get((int) estimate).getY() < queryPoint.getY() && estimate < points.size()) {
                estimate++;
            }
            while (estimate < points.size() && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    result.add(points.get((int) estimate));
                    return;
                }
                estimate++;
            }
        }
        // Scan in both directions
        else {
            long pos = estimate;
            // Scan till the end first
            while (estimate < points.size() && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    result.add(points.get((int) estimate));
                    return;
                }
                estimate++;
            }
            estimate = pos - 1;
            while (estimate >= 0 && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    result.add(points.get((int) estimate));
                    return;
                }
                estimate--;
            }
        }
    }
    public boolean pointLookUp(Point queryPoint){
        if (linear_scan) {
            for (Point point : points) {
                if (point.equals(queryPoint)) {

                    return true;
                }
            }
            return false;
        }
        if (!isBuild)
            // System.out.println("no BuildSpline");
            throw new AssertionError();

        // Estimate the from and to position using the spline
        long estimate = 0;
        if (queryPoint.getY() > min_y) {
            estimate = estimatePosition(queryPoint.getY());
        }

        // If we overshoot
        if (points.get((int) estimate).getY() > queryPoint.getY()) {
            while (points.get((int) estimate).getY() > queryPoint.getY() && estimate > 0) {
                estimate--;
            }
            while (estimate >= 0 && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    //result.add(points.get((int) estimate));
                    return true;
                }
                estimate--;
            }
        }
        // If RS underestimates
        else if (points.get((int) estimate).getY() < queryPoint.getY()) {
            while (points.get((int) estimate).getY() < queryPoint.getY() && estimate < points.size()) {
                estimate++;
            }
            while (estimate < points.size() && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    //result.add(points.get((int) estimate));
                    return true;
                }
                estimate++;
            }
        }
        // Scan in both directions
        else {
            long pos = estimate;
            // Scan till the end first
            while (estimate < points.size() && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    //result.add(points.get((int) estimate));
                    return true;
                }
                estimate++;
            }
            estimate = pos - 1;
            while (estimate >= 0 && points.get((int) estimate).getY() == queryPoint.getY()) {
                if (queryPoint.getX() == points.get((int) estimate).getX()) {
                    //result.add(points.get((int) estimate));
                    return true;
                }
                estimate--;
            }
        }
        return false;
    }

    public void lookUp(Rectangle rectangle, List<Point> result) {



        if (!isBuild) {
           // throw new AssertionError("isBuild is false!");
           // return ;

        }
        // Completely outside (only need to check y .. x is taken care of by the tree)
        if (rectangle.getFrom().getY() > max_y || rectangle.getTo().getY() < min_y) {
            return;
        }

        // Completely inside
        if (rectangle.getFrom().getX() <= min_x && max_x <= rectangle.getTo().getX()) {
            if (rectangle.getFrom().getY() <= min_y && max_y <= rectangle.getTo().getY()) {
                result.addAll(points);
                return;
            }

            if (linear_scan) {
                linearScanLookUp(rectangle, result);
                return;
            }

            int estimateTo = (int) estimateTo(rectangle);
            int estimateFrom = (int) estimateFrom(rectangle);
            if(estimateTo<estimateFrom)
            {
                int tmp = estimateFrom;
                estimateFrom = estimateTo;
                estimateTo = tmp;
            }
            result.addAll(points.subList(estimateFrom, estimateTo));
            return;
        }

        if (linear_scan) {
            linearScanLookUp(rectangle, result);
            return;
        }

        // Estimate the position using the spline
        if (rectangle.getFrom().getY() <= min_y) {
            lookUpUnderShot(rectangle, 0, result);
            return;
        }
        if (rectangle.getTo().getY() >= max_y) {
            lookUpOverShot(rectangle, points.size() - 1, result);
            return;
        }
        long estimate = estimatePosition((rectangle.getFrom().getY() + rectangle.getTo().getY()) / 2);

        // Case 1: overshoot completely, go down and then scan over data
        if (points.get((int) estimate).getY() > rectangle.getTo().getY()) {
            lookUpOverShot(rectangle, estimate, result);
            return;
        }

        // Case 2: undershot completely, go up and then scan over data
        if (points.get((int) estimate).getY() < rectangle.getFrom().getY()) {
            lookUpUnderShot(rectangle, estimate, result);
            return;
        }

        // Move back in case we overshot
        lookUpOverShot(rectangle, estimate, result);
        lookUpUnderShot(rectangle, estimate + 1, result);
    }

    public Rectangle BoundinBox(){return Utils.getBoundingBox(points);}

    public long getUsedMemory(Object obj) {
        // 初始内存使用
        long memory = 0;




        return memory;
    }

    public void add(Point point) {
        points.add(point);
    }
    public static long getLookupCount(List<Rectangle> rectangles) {
        return rectangles.size();
    }
    public void copyList(List<Point> result) {
        result.addAll(points);
    }

    public boolean checkDegenerate() {
        return points.get(0).equals(points.get(points.size() - 1));
    }




    private long estimatePosition(double val) {
        long segment = getSplineSegment(val);
        long estimate = (long) interpolateSegment(segment, val);

        if (estimate >= points.size()) {
            estimate = points.size() - 1;
        }

        return estimate;
    }

    private int getSplineSegment(double val) {
        if (val < min_y || val < spline.get(0).getX()) {
            // Handle the case when val is below the minimum value.
           // throw new IllegalStateException("val is below the minimum value.");
            return 0;

        }

        if (val > max_y || val > spline.get(spline.size() - 1).getX()) {
            //throw new IllegalStateException("val is above the maximum value.");
            // Handle the case when val is above the maximum value.
            return spline.size() - 1;
        }

        // Compute index.
        long p = transform(val);

        if (p >= radixHint.length) {
            // Handle the case when p exceeds the radixHint size.
            throw new IllegalStateException("p exceeds the radixHint size.");
            //return radixHint.size() - 1;
        }

        //int begin = radixHint.get((int) p);
        int begin = radixHint[(int) p];
        //int end = radixHint.get((int) p + 1);
        int end = radixHint[(int) p + 1];
        int index;

        if (end - begin == 0) {
            index = end;
        } else if (end - begin == 1) {
            index = (spline.get(begin).getX() > val) ? begin : end;
        } else if (end - begin == 2) {
            index = (spline.get(begin).getX() > val) ? begin :
                    (spline.get(begin + 1).getX() > val) ? (begin + 1) : end;
        } else if (end - begin == 3) {
            index = (spline.get(begin).getX() > val) ? begin :
                    (spline.get(begin + 1).getX() > val) ? (begin + 1) :
                            (spline.get(begin + 2).getX() > val) ? (begin + 2) : end;
        } else {
            int splineIndex = Collections.binarySearch(
                    spline.subList(begin, end),
                    new Coord(val, 0),
                    (a, lookupKey) -> Double.compare(a.getX(), lookupKey.getX()
                    ));
            index = (splineIndex >= 0) ? splineIndex + begin : -splineIndex - 1 + begin;
        }

        if (index == 0) {
            // Ensure index is not negative.
            index = 0;
        } else {
            index--;
        }

        if (index + 1 >= spline.size()) {
            // Ensure index does not exceed the spline size.
            index = spline.size() - 2;
        }

        // Check that we are on the correct segment.
        if (spline.get(index).getX() > val || val > spline.get(index + 1).getX()) {
            // Handle the case where val is outside the expected range.
            // You may add custom logic here or throw an exception.
            return -1;  // Adjust this to your error handling strategy.
        }

        return index;
    }

    private double interpolateSegment( long segment, double y) {
        if (segment + 1 >= spline.size()) {
            throw new IllegalStateException("Segment index is out of bounds.");
        }

        Coord down = spline.get((int)segment);
        Coord up = spline.get((int)segment + 1);

        if (down.getX() > y || y > up.getX()) {
            throw new IllegalArgumentException("Value y is outside the expected range.");
        }

        double slope = (down.getY() - up.getY()) / (down.getX() - up.getX());
        return Math.min(down.getY() + (y - down.getX()) * slope, up.getY());
    }

    private int linearScanCount(Rectangle rectangle) {
        int result = 0;
        for (Point p : points) {
            if (rectangle.getFrom().getX() <= p.getX() && p.getX() <= rectangle.getTo().getX() &&
                    rectangle.getFrom().getY() <= p.getY() && p.getY() <= rectangle.getTo().getY()) {
                result++;
            }
        }
        return result;
    }

    public void linearScanLookUp(Rectangle rectangle, List<Point> result) {
        for (Point p : points) {
            if (rectangle.getFrom().getX() <= p.getX() && p.getX() <= rectangle.getTo().getX() &&
                    rectangle.getFrom().getY() <= p.getY() && p.getY() <= rectangle.getTo().getY()) {
                result.add(p);
            }
        }
    }

    private int countOverShot(Rectangle rectangle, int estimate) {
        int result = 0;

        while (points.get(estimate).getY() > rectangle.getTo().getY() && estimate > 0) {
            estimate--;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        while (points.get(estimate).getY() >= rectangle.getFrom().getY() && estimate > 0) {
            if (rectangle.getFrom().getX() <= points.get(estimate).getX() &&
                    points.get(estimate).getX() <= rectangle.getTo().getX()) {
                result++;
            }
            estimate--;
            // 如果需要打印统计信息
            // actualScannedPointsCount++;
        }

        return result;
    }

    private int countUnderShot(Rectangle rectangle, int estimate) {
        int result = 0;

        while (estimate < points.size() && points.get(estimate).getY() < rectangle.getFrom().getY()) {
            estimate++;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        while (estimate < points.size() && points.get(estimate).getY() <= rectangle.getTo().getY()) {
            if (rectangle.getFrom().getX() <= points.get(estimate).getX() &&
                    points.get(estimate).getX() <= rectangle.getTo().getX()) {
                result++;
            }
            estimate++;
            // 如果需要打印统计信息
            // actualScannedPointsCount++;
        }

        return result;
    }

    private int countContained(Rectangle rectangle) {
        // 通过样条线估算 from 和 to 位置
        long estimateFrom = 0;
        if (rectangle.getFrom().getY() > min_y) {
            estimateFrom = estimatePosition(rectangle.getFrom().getY());
        }
        long estimateTo = points.size() - 1;
        if (rectangle.getTo().getY() < max_y) {
            estimateTo = estimatePosition(rectangle.getTo().getY());
        }

        // 调整 from 估算
        while (estimateFrom > 0 && points.get((int) estimateFrom).getY() > rectangle.getFrom().getY()) {
            estimateFrom--;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        while (estimateFrom < points.size() && points.get((int) estimateFrom).getY() < rectangle.getFrom().getY()) {
            estimateFrom++;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        // 调整 to 估算
        while (estimateTo > 0 && points.get((int) estimateTo).getY() > rectangle.getTo().getY()) {
            estimateTo--;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        while (estimateTo < points.size() && points.get((int) estimateTo).getY() < rectangle.getTo().getY()) {
            estimateTo++;
            // 如果需要打印统计信息
            // wronglyScannedPointsCount++;
        }

        // 如果需要打印统计信息
        // actualScannedPointsCount += (estimateTo - estimateFrom);

        return (int) (estimateTo - estimateFrom);
    }

    private long estimateFrom(Rectangle rectangle) {
        // 通过样条线估算 from 位置
        long estimateFrom = 0;
        if (rectangle.getFrom().getY() > min_y) {
            estimateFrom = estimatePosition(rectangle.getFrom().getY());
        }

        // 调整 from 估算
        while (estimateFrom > 0 && points.get((int) estimateFrom).getY() > rectangle.getFrom().getY()) {
            estimateFrom--;
        }

        while (estimateFrom < points.size() && points.get((int) estimateFrom).getY() < rectangle.getFrom().getY()) {
            estimateFrom++;
        }

        return estimateFrom;
    }

    private long estimateTo(Rectangle rectangle) {
        long estimateTo = points.size() - 1;
        if (rectangle.getTo().getY() < max_y) {
            estimateTo = estimatePosition(rectangle.getTo().getY());
        }

        // 调整 to 估算
        while (estimateTo > 0 && points.get((int) estimateTo).getY() > rectangle.getTo().getY()) {
            estimateTo--;
        }
        while (estimateTo < points.size() && points.get((int) estimateTo).getY() < rectangle.getTo().getY()) {
            estimateTo++;
        }

        return estimateTo;
    }

    private void lookUpOverShot(Rectangle rectangle, long estimate, List<Point> result) {


        while (estimate > 0 && points.get((int) estimate).getY() > rectangle.getTo().getY()) {
            estimate--;
        }

        while (estimate >= 0 && points.get((int) estimate).getY() >= rectangle.getFrom().getY()) {
            Point point = points.get((int) estimate);
            if (rectangle.getFrom().getX() <= point.getX() && point.getX() <= rectangle.getTo().getX()) {
                result.add(point);
            }
            estimate--;
        }
    }

    private void lookUpUnderShot(Rectangle rectangle, long estimate, List<Point> result) {

        while (estimate < points.size() && points.get((int) estimate).getY() < rectangle.getFrom().getY()) {
            estimate++;
        }

        while (estimate < points.size() && points.get((int) estimate).getY() <= rectangle.getTo().getY()) {
            Point point = points.get((int) estimate);
            if (rectangle.getFrom().getX() <= point.getX() && point.getX() <= rectangle.getTo().getX()) {
                result.add(point);
            }
            estimate++;
        }
    }


    public static int getSplineSize() {
        return SPLINE_SIZE;
    }

    public static int getRadixSize() {
        return RADIX_SIZE;
    }

    public boolean isBuild() {
        return isBuild;
    }

    public void setBuild(boolean build) {
        isBuild = build;
    }

    public boolean isLinear_scan() {
        return linear_scan;
    }

    public void setLinear_scan(boolean linear_scan) {
        this.linear_scan = linear_scan;
    }

    public List<Point> getPoints() {
        return points;
    }

    public List<Coord> getSpline() {
        return spline;
    }

    public void setSpline(List<Coord> spline) {
        this.spline = spline;
    }

    public int[] getRadixHint() {
        return radixHint;
    }

    public void setRadixHint(int[] radixHint) {
        this.radixHint = radixHint;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public void setMin_y(double min_y) {
        this.min_y = min_y;
    }

    public void setMax_y(double max_y) {
        this.max_y = max_y;
    }

    public void setMin_x(double min_x) {
        this.min_x = min_x;
    }

    public void setMax_x(double max_x) {
        this.max_x = max_x;
    }

    public double getFactor() {
        return factor;
    }

    public void setFactor(double factor) {
        this.factor = factor;
    }

    @Override
    public String toString() {
        return "Spline{" +
                "isBuild=" + isBuild +
                ", linear_scan=" + linear_scan +
                ", points=" + points +
                ", spline=" + spline +
                ", radixHint=" + Arrays.toString(radixHint) +
                ", n=" + n +
                ", min_y=" + min_y +
                ", max_y=" + max_y +
                ", min_x=" + min_x +
                ", max_x=" + max_x +
                ", factor=" + factor +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Spline spline1 = (Spline) o;
        return isBuild == spline1.isBuild &&
                linear_scan == spline1.linear_scan &&
                n == spline1.n &&
                Double.compare(spline1.min_y, min_y) == 0 &&
                Double.compare(spline1.max_y, max_y) == 0 &&
                Double.compare(spline1.min_x, min_x) == 0 &&
                Double.compare(spline1.max_x, max_x) == 0 &&
                Double.compare(spline1.factor, factor) == 0 &&
                points.equals(spline1.points) &&
                spline.equals(spline1.spline) &&
                Arrays.equals(radixHint, spline1.radixHint);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(isBuild, linear_scan, points, spline, n, min_y, max_y, min_x, max_x, factor);
        result = 31 * result + Arrays.hashCode(radixHint);
        return result;
    }

}

package utils;





import datatypes.Point;

import java.io.Serializable;
import java.util.Comparator;

public class PointDistanceComparator implements Comparator<Point>, Serializable {
    private final Point queryPoint;

    public PointDistanceComparator(Point queryPoint) {
        this.queryPoint = queryPoint;
    }

    @Override
    public int compare(Point p1, Point p2) {
        double distance1 = calculateDistance(p1);
        double distance2 = calculateDistance(p2);
        return Double.compare(distance1, distance2);
    }

    private double calculateDistance(Point p) {
        // 计算两点之间的欧氏距离
        double dx = p.getX() - queryPoint.getX();
        double dy = p.getY() - queryPoint.getY();
        return Math.sqrt(dx * dx + dy * dy);
    }
}

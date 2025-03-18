package utils;


import datatypes.Point;
import datatypes.Rectangle;

import java.util.Collections;
import java.util.List;

public class Utils {
    public static Rectangle getBoundingBox(List<Point> points) {
        double minX,minY,maxX,maxY;
        minX = points.get(0).getX();
        minY = points.get(0).getY();
        maxX = points.get(0).getX();
        maxY = points.get(0).getY();

        for (Point point : points) {
            if (point.getX() < minX) {
                minX = point.getX();
            }
            if (point.getX() > maxX) {
                maxX = point.getX();
            }
            if (point.getY() < minY) {
                minY = point.getY();
            }
            if (point.getY() > maxY) {
                maxY = point.getY();
            }
        }
        Rectangle rectangle = new Rectangle(minX,minY,maxX,maxY);
        return rectangle;
    }

    public static void sortPointsY(List<Point> points) {
        Collections.sort(points, (p1, p2) -> Double.compare(p1.getY(), p2.getY()));
    }

    public static int calculatePartitionId(Point point, double boundingBoxX, double gridStep) {
        return (int) ((point.getX() - boundingBoxX) / gridStep);
    }
    public static Point generateJTSPoint(double x, double y){
        /*Coordinate coordinate = new Coordinate(x, y);
        GeometryFactory geometryFactory = new GeometryFactory();

        Point point = geometryFactory.createPoint(coordinate);*/
        Point point = new Point(x,y);
        return  point;
    }

}

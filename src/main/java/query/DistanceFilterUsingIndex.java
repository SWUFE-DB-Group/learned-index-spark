package query;


import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.function.FlatMapFunction;
import spline.Spline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DistanceFilterUsingIndex implements FlatMapFunction<Iterator<Spline>, Point> {
    final double EARTH_RADIUS_METERS = 6378137.0;
    double distance;
    Point query_point;
    Rectangle queryRange;

    public DistanceFilterUsingIndex(Point query_point, double distance) {
        this.distance = distance;
        this.query_point = query_point;
    }

    List<Point> distanceQuery(List<Point> points, Point queryPoint, double distance) {
        List<Point> result = new ArrayList<>();
        double lat1r = deg2rad(queryPoint.getX());
        double lon1r = deg2rad(queryPoint.getY());
        for (Point point : points) {
            double lat2r = deg2rad(point.getX());
            double lon2r = deg2rad(point.getY());
            if (haversineDistance(lat1r, lon1r, lat2r, lon2r) <= distance)
                result.add(point);
        }
        return result;
    }


    double deg2rad(double deg) {
        return deg * Math.PI / 180.0;
    }
    double rad2deg(double rad) {
        return rad * 180.0 / Math.PI;
    }
    double haversineDistance(double lat1r, double lon1r, double lat2r, double lon2r) {
        double u = Math.sin((lat2r - lat1r) / 2);
        double v = Math.sin((lon2r - lon1r) / 2);
        return 2.0 * EARTH_RADIUS_METERS * Math.asin(Math.sqrt(u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v));
    }

    @Override
    public Iterator<Point> call(Iterator<Spline> splineIterator) throws Exception {
        List<Point> rangePoints = new ArrayList<>();
        List<Point> res = new ArrayList<>();
        double x,y;
        x = query_point.getX();
        y = query_point.getY();
        Point from = new Point(x-distance,y-distance);
        Point to = new Point(x+distance,y+distance);

        queryRange = new Rectangle(from,to);
        if(splineIterator.hasNext())
        {
            Spline spline = splineIterator.next();

            spline.lookUp(queryRange,rangePoints);
            res = distanceQuery(rangePoints, query_point, distance);

        }
        return res.iterator();
    }
}

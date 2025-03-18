package query;


import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;
import spline.Spline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class JoinQueryUsingIndex implements PairFlatMapFunction<Iterator<Spline>, Polygon, List<Point>> {

    List<Polygon> polygons;

    public JoinQueryUsingIndex(List<Polygon> polygons) {
        this.polygons = polygons;
    }
    /*private List<Tuple2<Polygon,List<Point> >> joinQuery(List<Point> points, Polygon polygon) {
        List<Tuple2<Polygon,List<Point>>> result = new ArrayList<>();
        List<Point>resp = new ArrayList<>();
        GeometryFactory geometryFactory = new GeometryFactory();
        for (Point point : points) {
            org.locationtech.jts.geom.Point p = geometryFactory.createPoint(new Coordinate(point.getX(), point.getY()));
            if (polygon.contains(p)) {
                resp.add(point);
            }
        }
        result.add(new Tuple2<>(polygon,resp));
        return result;
    }*/
    @Override
    public Iterator<Tuple2< Polygon,List<Point>>> call(Iterator<Spline> splineIterator) throws Exception {
        List<Tuple2<Polygon,List<Point>>> finalResult = new ArrayList<>();
        List<Point> rangePoints = new ArrayList<>();

        if (splineIterator.hasNext()) {
            Spline spline = splineIterator.next();
            for (Polygon polygon : polygons) {
                Envelope envelope = polygon.getEnvelopeInternal();
                Rectangle queryRange = new Rectangle(envelope);
                //spline.lookUp(queryRange, rangePoints);
                spline.linearScanLookUp(queryRange,rangePoints);
                //List<Tuple2<Polygon, List<Point>>> tuple2s = joinQuery(rangePoints, polygon);
                Tuple2<Polygon, List<Point>>tuple2 = new Tuple2<>(polygon,rangePoints);
                finalResult.addAll(Collections.singleton(tuple2));
            }
        }

        return finalResult.iterator();
    }

}

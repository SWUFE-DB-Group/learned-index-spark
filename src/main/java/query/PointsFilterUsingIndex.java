package query;


import datatypes.Point;
import org.apache.spark.api.java.function.FlatMapFunction;
import spline.Spline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PointsFilterUsingIndex implements FlatMapFunction<Iterator<Spline>, Point> {
    List<Point> pointList;
    Point point;
    public PointsFilterUsingIndex(){
        this.pointList = new ArrayList<>();
    }
    public PointsFilterUsingIndex(Point p){
        this.pointList = new ArrayList<>();
        this.point = p;
    }
    @Override
    public Iterator<Point> call(Iterator<Spline> splineIterator) throws Exception {
        while(splineIterator.hasNext())
        {
            Spline spline = splineIterator.next();
            spline.pointLookUp(point,pointList);
        }
        return pointList.iterator();
    }
}

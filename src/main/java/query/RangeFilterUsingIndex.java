package query;

import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.function.FlatMapFunction;
import spline.Spline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RangeFilterUsingIndex implements FlatMapFunction<Iterator<Spline>, Point> {
    private final Rectangle queryRange;

    public RangeFilterUsingIndex(Rectangle queryRange) {
        this.queryRange = queryRange;
    }


    @Override
    public Iterator<Point> call(Iterator<Spline> splineIterator) throws Exception {
        List<Point> res = new ArrayList<>();
        if(splineIterator.hasNext())
        {
            Spline spline = splineIterator.next();
            spline.lookUp(queryRange,res);
            //spline.linearScanLookUp(queryRange,res);
        }
        return res.iterator();
    }
}

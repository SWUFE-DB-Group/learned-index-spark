package index;


import datatypes.Point;
import org.apache.spark.api.java.function.FlatMapFunction;
import spline.Spline;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IndexBuilder implements FlatMapFunction<Iterator<Point>, Spline> {

    public IndexBuilder(){

    }


    @Override
    public Iterator<Spline> call(Iterator<Point> pointIterator) throws Exception {
       // long startTime1 = System.currentTimeMillis();
        Spline spline = new Spline();
        while(pointIterator.hasNext())
        {
            Point p =pointIterator.next();
            spline.add(p);
        }
        spline.build();

       // System.out.println(spline.isBuild());
       // System.out.println(spline.getPoints().size());
        Set<Spline> result = new HashSet<>();
        //spline.pointLookUp(new Point(-87.629331816,41.852298777));
      //  long endTime1 = System.currentTimeMillis();
        /*System.out.println("++++++++++++++++");
        System.out.println(endTime1-startTime1+"ms");*/
        result.add(spline);

        return result.iterator();
    }
}

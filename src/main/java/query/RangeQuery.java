package query;


import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import spline.Spline;

import java.io.Serializable;

public class RangeQuery implements Serializable {
    RangeQuery() {
    }

    public static JavaRDD<Point> SpatialRangeQuery(JavaRDD<Spline> pointRDD, Rectangle rec) {
        JavaRDD<Spline> filterdd = pointRDD.filter(new Function<Spline, Boolean>() {
            @Override
            public Boolean call(Spline v1) throws Exception {
                /*List<Point>queryPoint = new ArrayList<>();
                v1.lookUp(rec,queryPoint);*/
                double max_x = v1.getMax_x();
                double min_x = v1.getMin_x();
                double min_y = v1.getMin_y();
                double max_y = v1.getMax_y();
                Rectangle rectangle = new Rectangle(min_x,min_y,max_x,max_y);
                //boolean contains = rec.intersects(rectangle);
                if (rec.contains(rectangle)||rec.intersects(rectangle)) {

                    return true;
                }
                return false;
            }
        });
        //List<Spline> take = filterdd.take(0);
        JavaRDD<Point> pointJavaRDD = filterdd.mapPartitions(new RangeFilterUsingIndex(rec),true);
        return pointJavaRDD;
    }
    public static JavaRDD<Point> SpatialRangeQueryWithOutIndex(JavaRDD<Point> pointRDD, Rectangle rec) {
        JavaRDD<Point> resultRDD = pointRDD.filter(point -> rec.contains(point.getX(), point.getY()));
        return resultRDD;
    }
}

package query;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import spline.Spline;

public class DistanceQuery {
    DistanceQuery() {
    }
    public static JavaRDD<Point> SpatialDistanceQuery(JavaRDD<Spline> pointRDD, Point queryPoint, double distance){
        double min_x,max_x,min_y,max_y;
        min_x = queryPoint.getX()-distance;
        max_x = queryPoint.getX()+distance;
        min_y = queryPoint.getY()-distance;
        max_y = queryPoint.getY()+distance;

        JavaRDD<Spline> filterdd = pointRDD.filter(new Function<Spline, Boolean>() {
            @Override
            public Boolean call(Spline v1) throws Exception {
                if(max_x<v1.getMin_x()||min_x>v1.getMax_x()
                        ||max_y<v1.getMin_y()||min_y>v1.getMax_y())return false;
                else return true;
            }
        });
        JavaRDD<Point> pointJavaRDD = filterdd.mapPartitions(new DistanceFilterUsingIndex(queryPoint,distance));
        return pointJavaRDD;
    }
}

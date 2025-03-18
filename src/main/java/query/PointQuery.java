package query;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import spline.Spline;

public class PointQuery {
    PointQuery(){}
    public static JavaRDD<Point> SpatialPointQuery(JavaRDD<Spline> indexRDD, Point queryPoint){

        JavaRDD<Spline> filterdd = indexRDD.filter(new Function<Spline, Boolean>() {


            @Override
            public Boolean call(Spline v1) throws Exception {
                double x = queryPoint.getX();
                double y = queryPoint.getY();
                double max_y = v1.getMax_y();
                double min_y = v1.getMin_y();
                double min_x = v1.getMin_x();
                double max_x = v1.getMax_x();
                if(x<=max_x&&x>=min_x&&y<=max_y&&y>=min_y)return true;

                return false;
              /* return areRectanglesIntersecting(v1.getMax_x(),v1.getMax_y(),v1.getMin_x(),v1.getMin_y(),
                        envelopeInternal.getMaxX(),envelopeInternal.getMaxY(),
                        envelopeInternal.getMinX(),envelopeInternal.getMinY());*/
            }
        });

        JavaRDD<Point> resultRDD = filterdd.mapPartitions(new PointsFilterUsingIndex(queryPoint));
        return  resultRDD;
    }

}

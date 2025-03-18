package index;


import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import spline.Spline;

public class BuildIndex {
    public static JavaRDD<Spline> indexBuild(JavaRDD<Point>pointJavaRDD){
        return pointJavaRDD.mapPartitions(new IndexBuilder());
    }

}

package query;

import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;
import spline.Spline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JoinQuery implements Serializable {
    // 默认构造函数
    JoinQuery() {
    }

    // 静态方法，进行空间连接查询
    public static void SpatialJoinQuery(JavaRDD<Spline> indexRDD, List<Polygon> polygons,boolean writeb) {
        // 将多边形转为近似的矩形范围


        for (Polygon polygon : polygons) {
            Envelope envelope = polygon.getEnvelopeInternal();
            Rectangle queryRange = new Rectangle(envelope);

            JavaRDD<Point> pointJavaRDD = RangeQuery.SpatialRangeQuery(indexRDD, queryRange);
            long count = pointJavaRDD.count();


        }
        if(writeb)
        {

        }

    }
    public static void SpatialJoinQueryWitoutIndex(JavaRDD<Point> indexRDD, List<Polygon> polygons,boolean writeb) {
        // 将多边形转为近似的矩形范围
        List<Tuple2<Polygon,List<Point>>> finalResult = new ArrayList<>();

        for (Polygon polygon : polygons) {
            Envelope envelope = polygon.getEnvelopeInternal();
            Rectangle queryRange = new Rectangle(envelope);

            JavaRDD<Point> pointJavaRDD = RangeQuery.SpatialRangeQueryWithOutIndex(indexRDD, queryRange);
            long count = pointJavaRDD.count();
        }
        if(writeb)
        {

        }
    }
}
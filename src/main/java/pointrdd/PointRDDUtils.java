package pointrdd;

import datatypes.Point;
import datatypes.Rectangle;
import javafx.util.Pair;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import org.locationtech.jts.geom.*;
import utils.StatCalculator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PointRDDUtils {
    public static JavaRDD<Point>CreatePointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset){
        JavaRDD<String> csvFile = sparkContext.textFile(InputLocation);
        JavaRDD<List<String>> parsedCSV = csvFile.map(line -> Arrays.asList(line.split(",")));
        JavaRDD<Point> pointsRDD = parsedCSV.map(fields -> {
            double x = Double.parseDouble(fields.get(Offset));  // 假设第一个字段是 X 坐标
            double y = Double.parseDouble(fields.get(Offset+1));  // 假设第二个字段是 Y 坐标
            return new Point(x, y);
        });
        return pointsRDD;
    }
    public static JavaRDD<Polygon> createPolygonRDD(JavaSparkContext sparkContext, String inputLocation) {
        // 读取CSV文件
        JavaRDD<String> csvFile = sparkContext.textFile(inputLocation);

        // 将CSV的每一行解析为字段列表
        JavaRDD<List<String>> parsedCSV = csvFile.map(line -> Arrays.asList(line.split(",")));

        // 将每5行数据分组为一个多边形
        JavaRDD<Polygon> polygonsRDD = parsedCSV
                .mapPartitions(new FlatMapFunction<Iterator<List<String>>, Polygon>() {
                    @Override
                    public Iterator<Polygon> call(Iterator<List<String>> iterator) {
                        List<Polygon> polygons = new ArrayList<>();
                        List<Coordinate> coordinates = new ArrayList<>();
                        GeometryFactory geometryFactory = new GeometryFactory();

                        while (iterator.hasNext()) {
                            List<String> fields = iterator.next();
                            double x = Double.parseDouble(fields.get(1));
                            double y = Double.parseDouble(fields.get(2));
                            coordinates.add(new Coordinate(x, y));

                            if (coordinates.size() == 5) {
                                // Ensure the polygon is closed
                                if (!coordinates.get(0).equals(coordinates.get(4))) {
                                    coordinates.add(coordinates.get(0)); // Close the polygon
                                }

                                // Convert List to array for JTS
                                Coordinate[] coordArray = coordinates.toArray(new Coordinate[0]);

                                // Create the LinearRing and Polygon
                                LinearRing linearRing = geometryFactory.createLinearRing(coordArray);
                                Polygon polygon = geometryFactory.createPolygon(linearRing);

                                polygons.add(polygon);

                                // Clear for the next polygon
                                coordinates.clear();
                            }
                        }
                        return polygons.iterator();
                    }
                });

        return polygonsRDD;
    }



    public static Pair<Long,Rectangle> PointRDDanalyze(JavaRDD<Point>pointRDD){
        Rectangle box = null;
        long approximateTotalCount;

        Function2 combOp = new Function2<StatCalculator, StatCalculator, StatCalculator>() {
            public StatCalculator call(StatCalculator agg1, StatCalculator agg2) throws Exception {
                return StatCalculator.combine(agg1, agg2);
            }
        };
        Function2 seqOp = new Function2<StatCalculator, Point, StatCalculator>() {
            public StatCalculator call(StatCalculator agg, Point object) throws Exception {
                return StatCalculator.add(agg, object);
            }
        };
        StatCalculator agg = (StatCalculator)pointRDD.aggregate((Object)null, seqOp, combOp);
        if (agg != null) {
            Envelope boundary = agg.getBoundary();
            box = new Rectangle(boundary);
            approximateTotalCount = agg.getCount();
        } else {
            box = null;
            approximateTotalCount = 0L;
        }
        return new Pair<>(approximateTotalCount,box);
    }
}

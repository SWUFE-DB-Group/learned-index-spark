package sql;

import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import partitions.SpatialPartition;
import query.DistanceQuery;
import query.KNNQuery;
import query.PointQuery;
import query.RangeQuery;
import scala.Tuple2;
import spline.Spline;
import utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LiLIS {
    public static final String DEFAULT_PARTITION_METHOD = "QuadTree";

    private static final StructType POINT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("x", DataTypes.DoubleType, false),
            DataTypes.createStructField("y", DataTypes.DoubleType, false)
    });

    private static final StructType JOIN_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("polygon_id", DataTypes.LongType, false),
            DataTypes.createStructField("x", DataTypes.DoubleType, false),
            DataTypes.createStructField("y", DataTypes.DoubleType, false)
    });

    private LiLIS() {
    }

    public static void register(SparkSession spark) {
        LiLISFunctions.registerAll(spark);
    }

    public static IndexedSpatialRDD createIndexAsView(SparkSession spark, Dataset<Row> df,
                                                      String viewName, String partitionMethod) throws Exception {
        IndexedSpatialRDD index = createIndex(spark, df, partitionMethod);
        LiLISIndexRegistry.register(viewName, index);
        df.createOrReplaceTempView(viewName);
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW " + viewName + "_lilis USING lilis OPTIONS (index_name '" + viewName + "')");
        return index;
    }

    public static IndexedSpatialRDD createIndexAsView(SparkSession spark, Dataset<Row> df,
                                                      String viewName) throws Exception {
        return createIndexAsView(spark, df, viewName, DEFAULT_PARTITION_METHOD);
    }

    public static void rangeQueryAsView(SparkSession spark, String indexName, String resultViewName,
                                        double xMin, double yMin, double xMax, double yMax) {
        Dataset<Row> result = rangeQuery(spark, LiLISIndexRegistry.get(indexName), xMin, yMin, xMax, yMax);
        result.createOrReplaceTempView(resultViewName);
    }

    public static void pointQueryAsView(SparkSession spark, String indexName, String resultViewName,
                                        double queryX, double queryY) {
        Dataset<Row> result = pointQuery(spark, LiLISIndexRegistry.get(indexName), queryX, queryY);
        result.createOrReplaceTempView(resultViewName);
    }

    public static void knnQueryAsView(SparkSession spark, String indexName, String resultViewName,
                                      double queryX, double queryY, int k) {
        Dataset<Row> result = knnQuery(spark, LiLISIndexRegistry.get(indexName), queryX, queryY, k);
        result.createOrReplaceTempView(resultViewName);
    }

    public static void distanceQueryAsView(SparkSession spark, String indexName, String resultViewName,
                                           double queryX, double queryY, double distance) {
        Dataset<Row> result = distanceQuery(spark, LiLISIndexRegistry.get(indexName), queryX, queryY, distance);
        result.createOrReplaceTempView(resultViewName);
    }

    public static Dataset<Row> loadCSV(SparkSession spark, String path) {
        return loadCSV(spark, path, 0, 1, true);
    }

    public static Dataset<Row> loadCSV(SparkSession spark, String path, int xCol, int yCol, boolean hasHeader) {
        Dataset<Row> raw = spark.read()
                .option("header", String.valueOf(hasHeader))
                .option("inferSchema", "true")
                .csv(path);
        String[] columns = raw.columns();
        if (xCol < 0 || xCol >= columns.length || yCol < 0 || yCol >= columns.length) {
            throw new IllegalArgumentException("xCol/yCol are outside CSV column bounds");
        }
        return raw.select(raw.col(columns[xCol]).cast(DataTypes.DoubleType).alias("x"),
                raw.col(columns[yCol]).cast(DataTypes.DoubleType).alias("y"));
    }

    public static IndexedSpatialRDD createIndex(SparkSession spark, Dataset<Row> df) throws Exception {
        return createIndex(spark, df, DEFAULT_PARTITION_METHOD);
    }

    public static IndexedSpatialRDD createIndex(SparkSession spark, Dataset<Row> df, String partitionMethod) throws Exception {
        JavaRDD<Point> pointRDD = toPointRDD(df).persist(StorageLevel.MEMORY_AND_DISK());
        List<Point> points = pointRDD.collect();
        Rectangle boundingBox = Utils.getBoundingBox(points);
        long totalCount = points.size();
        JavaRDD<Point> partitionedRDD = partition(pointRDD, partitionMethod);
        JavaRDD<Spline> indexRDD = BuildIndex.indexBuild(partitionedRDD).persist(StorageLevel.MEMORY_AND_DISK());
        indexRDD.count();
        return new IndexedSpatialRDD(indexRDD, boundingBox, totalCount, normalizePartitionMethod(partitionMethod));
    }

    public static Dataset<Row> rangeQuery(SparkSession spark, IndexedSpatialRDD index,
                                          double xMin, double yMin, double xMax, double yMax) {
        Rectangle queryRange = normalizeRectangle(xMin, yMin, xMax, yMax);
        JavaRDD<Point> resultRDD = RangeQuery.SpatialRangeQuery(index.getIndexRDD(), queryRange);
        return toPointDataFrame(spark, resultRDD);
    }

    public static Dataset<Row> pointQuery(SparkSession spark, IndexedSpatialRDD index, double queryX, double queryY) {
        JavaRDD<Point> resultRDD = PointQuery.SpatialPointQuery(index.getIndexRDD(), new Point(queryX, queryY));
        return toPointDataFrame(spark, resultRDD);
    }

    public static Dataset<Row> knnQuery(SparkSession spark, IndexedSpatialRDD index, double queryX, double queryY, int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be greater than 0");
        }
        double maxArea = index.getBoundingBox() == null ? 0.0 : index.getBoundingBox().getArea();
        List<Point> points = KNNQuery.SpatialKNNQuery(index.getIndexRDD(), k, new Point(queryX, queryY), maxArea, index.getTotalCount());
        return toPointDataFrame(spark, new JavaSparkContext(spark.sparkContext()).parallelize(points));
    }

    public static Dataset<Row> distanceQuery(SparkSession spark, IndexedSpatialRDD index,
                                             double queryX, double queryY, double distance) {
        if (distance < 0) {
            throw new IllegalArgumentException("distance must be non-negative");
        }
        JavaRDD<Point> resultRDD = DistanceQuery.SpatialDistanceQuery(index.getIndexRDD(), new Point(queryX, queryY), distance);
        return toPointDataFrame(spark, resultRDD);
    }

    public static Dataset<Row> spatialJoin(SparkSession spark, IndexedSpatialRDD index, Dataset<Row> polygonDF) {
        List<Tuple2<Long, Polygon>> polygons = collectPolygons(polygonDF);
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        List<Row> rows = new ArrayList<>();
        for (Tuple2<Long, Polygon> entry : polygons) {
            Envelope envelope = entry._2().getEnvelopeInternal();
            Rectangle queryRange = new Rectangle(envelope);
            List<Point> points = RangeQuery.SpatialRangeQuery(index.getIndexRDD(), queryRange).collect();
            for (Point point : points) {
                rows.add(RowFactory.create(entry._1(), point.getX(), point.getY()));
            }
        }
        return spark.createDataFrame(jsc.parallelize(rows), JOIN_SCHEMA);
    }

    public static Dataset<Row> toPointDataFrame(SparkSession spark, JavaRDD<Point> pointRDD) {
        JavaRDD<Row> rowRDD = pointRDD.map(point -> RowFactory.create(point.getX(), point.getY()));
        return spark.createDataFrame(rowRDD, POINT_SCHEMA);
    }

    public static JavaRDD<Point> toPointRDD(Dataset<Row> df) {
        return df.select(df.col("x").cast(DataTypes.DoubleType), df.col("y").cast(DataTypes.DoubleType))
                .javaRDD()
                .map(row -> new Point(row.getDouble(0), row.getDouble(1)));
    }

    private static JavaRDD<Point> partition(JavaRDD<Point> pointRDD, String partitionMethod) throws Exception {
        String method = normalizePartitionMethod(partitionMethod);
        switch (method) {
            case "QuadTree":
                return SpatialPartition.QuadtreePartitioner(pointRDD);
            case "KDBTree":
                return SpatialPartition.KDBTreePartitioner(pointRDD);
            case "RTree":
                return SpatialPartition.RtreePartitoner(pointRDD);
            case "FixGrid":
                return SpatialPartition.FixGridPartitioner(pointRDD);
            case "AdaptiveGrid":
                return SpatialPartition.AdaptiveGridPartitioner(pointRDD);
            default:
                throw new IllegalArgumentException("Unsupported partition method: " + partitionMethod);
        }
    }

    private static String normalizePartitionMethod(String partitionMethod) {
        if (partitionMethod == null || partitionMethod.trim().isEmpty()) {
            return DEFAULT_PARTITION_METHOD;
        }
        String method = partitionMethod.trim().toLowerCase();
        if (method.equals("quad") || method.equals("quadtree")) {
            return "QuadTree";
        }
        if (method.equals("kdb") || method.equals("kdbtree") || method.equals("kd-tree")) {
            return "KDBTree";
        }
        if (method.equals("r") || method.equals("rtree") || method.equals("r-tree")) {
            return "RTree";
        }
        if (method.equals("fixgrid") || method.equals("fixedgrid") || method.equals("grid")) {
            return "FixGrid";
        }
        if (method.equals("adaptivegrid") || method.equals("adaptive")) {
            return "AdaptiveGrid";
        }
        return partitionMethod;
    }

    private static Rectangle normalizeRectangle(double xMin, double yMin, double xMax, double yMax) {
        return new Rectangle(Math.min(xMin, xMax), Math.min(yMin, yMax), Math.max(xMin, xMax), Math.max(yMin, yMax));
    }

    private static List<Tuple2<Long, Polygon>> collectPolygons(Dataset<Row> polygonDF) {
        List<Tuple2<Long, Polygon>> polygons = new ArrayList<>();
        List<Row> rows = polygonDF.collectAsList();
        WKTReader reader = new WKTReader();
        String[] columns = polygonDF.columns();
        List<String> columnNames = Arrays.asList(columns);
        boolean hasWkt = columnNames.contains("wkt");
        boolean hasEnvelope = columnNames.contains("xmin") && columnNames.contains("ymin")
                && columnNames.contains("xmax") && columnNames.contains("ymax");
        if (!hasWkt && !hasEnvelope) {
            throw new IllegalArgumentException("polygonDF must contain either a wkt column or xmin/ymin/xmax/ymax columns");
        }
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            long polygonId = columnNames.contains("polygon_id") ? getLong(row, "polygon_id", columnNames) : i;
            try {
                if (hasWkt) {
                    Object geometry = reader.read(row.getAs("wkt").toString());
                    if (geometry instanceof Polygon) {
                        polygons.add(new Tuple2<>(polygonId, (Polygon) geometry));
                    }
                } else {
                    double xmin = getDouble(row, "xmin", columnNames);
                    double ymin = getDouble(row, "ymin", columnNames);
                    double xmax = getDouble(row, "xmax", columnNames);
                    double ymax = getDouble(row, "ymax", columnNames);
                    polygons.add(new Tuple2<>(polygonId, rectangleAsPolygon(normalizeRectangle(xmin, ymin, xmax, ymax))));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse polygon row " + i, e);
            }
        }
        return polygons;
    }

    private static Polygon rectangleAsPolygon(Rectangle rectangle) {
        org.locationtech.jts.geom.GeometryFactory geometryFactory = new org.locationtech.jts.geom.GeometryFactory();
        org.locationtech.jts.geom.Coordinate[] coordinates = new org.locationtech.jts.geom.Coordinate[]{
                new org.locationtech.jts.geom.Coordinate(rectangle.getminX(), rectangle.getminY()),
                new org.locationtech.jts.geom.Coordinate(rectangle.getmaxX(), rectangle.getminY()),
                new org.locationtech.jts.geom.Coordinate(rectangle.getmaxX(), rectangle.getmaxY()),
                new org.locationtech.jts.geom.Coordinate(rectangle.getminX(), rectangle.getmaxY()),
                new org.locationtech.jts.geom.Coordinate(rectangle.getminX(), rectangle.getminY())
        };
        return geometryFactory.createPolygon(coordinates);
    }

    private static double getDouble(Row row, String column, List<String> columns) {
        Object value = row.get(columns.indexOf(column));
        return ((Number) value).doubleValue();
    }

    private static long getLong(Row row, String column, List<String> columns) {
        Object value = row.get(columns.indexOf(column));
        return ((Number) value).longValue();
    }
}

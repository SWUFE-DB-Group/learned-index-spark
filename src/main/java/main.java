import datatypes.Point;
import datatypes.Rectangle;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import partitions.AdaptiveGridPartitioner;
import partitions.SpatialPartition;
import query.RangeQuery;
import spline.Spline;
import utils.ReadPoints;
import utils.Utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class main {
    public static void main(String[] args) throws Exception {
        String jsonFilePath = "./yourfilename";
        List<Point> points = new ArrayList<>();
        System.out.println("loading...");
        ReadPoints.readPointsToJson(jsonFilePath,points);
        Rectangle boundingBox = Utils.getBoundingBox(points);
        double GRID_STEP;
        double boundingBoxX;
        GRID_STEP = (boundingBox.getTo().getX() - boundingBox.getFrom().getX()) / 1000;
        boundingBoxX = boundingBox.getFrom().getX();



        System.setProperty("hadoop.home.dir", "...");
        SparkConf conf = new SparkConf() .setMaster("local[*]").setAppName("SparkApp");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        // SparkSession spark = SparkSession.builder().appName("SparkApp").getOrCreate();
        /*Point p1 = Utils.generateJTSPoint(-87.697249968,41.822730356);
        Point p2 = Utils.generateJTSPoint(-87.686513789,41.830143053);*/


        JavaRDD<Point> pointRDD = jsc.parallelize(points);


        //partition
        //JavaRDD<Point> partitionRDD = SpatialPartition.KDBTreePartitioner(pointRDD);
        JavaRDD<Point> partitionRDD = SpatialPartition.QuadtreePartitioner(pointRDD);
        //JavaRDD<Point> partitionRDD = SpatialPartition.RtreePartitoner(pointRDD);
        //JavaRDD<Point> partitionRDD = FixGridPartitioner.partitionPoints(pointRDD, boundingBoxX, GRID_STEP, 100);
        //JavaRDD<Point> partitionRDD = AdaptiveGridPartitioner.partitionPoints(pointRDD, boundingBox, 100);

        //build index
        long startTime = System.currentTimeMillis();
        JavaRDD<Spline> splineJavaRDD = BuildIndex.indexBuild(partitionRDD);

        List<Spline> collect1 = splineJavaRDD.take(2);
        long endTime = System.currentTimeMillis();

        List<Long> resultsize = new ArrayList<>();
        //query

        long startTime1 = System.currentTimeMillis();


        Rectangle rec = new Rectangle(-87.695865725,41.850202933,-87.625438506,  41.87053323);
        JavaRDD<Point> pointJavaRDD = RangeQuery.SpatialRangeQuery(splineJavaRDD, boundingBox);
        long count = pointJavaRDD.count();
        long endTime1 = System.currentTimeMillis();





        System.out.println("indexBuild spend time = "+(endTime-startTime)+"ms");

        System.out.println("query spend time = "+(endTime1-startTime1)+"ms");

        System.out.println(boundingBox);

    }

}

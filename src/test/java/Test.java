import datatypes.Point;
import index.BuildIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import partitions.SpatialPartition;
import pointrdd.PointRDDUtils;
import spline.Spline;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static utils.ReadPoints.readPointsFromCSV;

public class Test {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf()
                         .setAppName("SparkApp");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Point> pointRDD = PointRDDUtils.CreatePointRDD(sc, args[1], 0);

        long startTime = System.currentTimeMillis();

        JavaRDD<Point> partitionRDD = SpatialPartition.QuadtreePartitioner(pointRDD);
        JavaRDD<Spline> splineJavaRDD = BuildIndex.indexBuild(partitionRDD);
        long count = splineJavaRDD.count();
        long endTime = System.currentTimeMillis();




        try (BufferedWriter writer = new BufferedWriter(new FileWriter(  "/home/hwj/result.txt",true))) {
            writer.write("--------------------------------------");
            writer.write(args[0]);
            writer.write("build Index:"+"\n");
            writer.write("Index build time : " + (endTime-startTime)+ "\n");


        } catch (IOException e) {
            e.printStackTrace();
        }


        sc.close();
    }
}

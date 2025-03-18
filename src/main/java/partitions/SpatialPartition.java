package partitions;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public class SpatialPartition implements Serializable {
    public static JavaRDD<Point> QuadtreePartitioner(JavaRDD<Point> pointRDD) throws Exception {

        QuadTreePartitioner quadTreePartitioner = new QuadTreePartitioner(pointRDD);
        int numPartitions = pointRDD.rdd().partitions().length;
        quadTreePartitioner.analyze();
        //System.out.println("numPartitions = "+numPartitions);
        JavaRDD<Point> pointJavaRDD = quadTreePartitioner.partitionPoints(numPartitions);
        return  pointJavaRDD;
    }
    public static JavaRDD<Point> KDBTreePartitioner(JavaRDD<Point> pointRDD) throws Exception {

        int numPartitions = pointRDD.rdd().partitions().length;
        KDBTreePartitioner kdbTreePartitioner = new KDBTreePartitioner(pointRDD);
        //System.out.println("numPartitions = "+numPartitions);
        kdbTreePartitioner.analyze();
        JavaRDD<Point> pointJavaRDD = kdbTreePartitioner.partitionPoints(numPartitions);
        return  pointJavaRDD;
    }
    public static JavaRDD<Point> RtreePartitoner(JavaRDD<Point> pointRDD)throws Exception {
        int numPartitions = pointRDD.rdd().partitions().length;
        RtreePatitioner rtreePatitioner = new RtreePatitioner(pointRDD);
        rtreePatitioner.analyze();
        //System.out.println("numPartitions = "+numPartitions);
        JavaRDD<Point> pointJavaRDD = rtreePatitioner.partitionPoints(numPartitions);
        return pointJavaRDD;
    }

    public static JavaRDD<Point> FixGridPartitioner(JavaRDD<Point> pointRDD)throws Exception{
        int numPartitions = 100;
        FixGridPartitioner fixGridPartitioner = new FixGridPartitioner(pointRDD);
        fixGridPartitioner.analyze();
        JavaRDD<Point> pointJavaRDD = fixGridPartitioner.partitionPoints(numPartitions);
        return pointJavaRDD;
    }
    public static JavaRDD<Point> AdaptiveGridPartitioner(JavaRDD<Point> pointRDD)throws Exception{
        int numPartitions = 100;
        AdaptiveGridPartitioner adaptiveGridPartitioner = new AdaptiveGridPartitioner(pointRDD);
        adaptiveGridPartitioner.analyze();
        JavaRDD<Point> pointJavaRDD = adaptiveGridPartitioner.partitionPoints(numPartitions);
        return pointJavaRDD;
    }
}

package benchmark;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import partitions.SpatialPartition;

public class KDBTreeBenchmark extends BenchmarkBase {

    @Override
    public String getPartitionName() {
        return "KDBTree";
    }

    @Override
    public JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception {
        return SpatialPartition.KDBTreePartitioner(pointRDD);
    }

    public static void main(String[] args) throws Exception {
        new KDBTreeBenchmark().run(args);
    }
}

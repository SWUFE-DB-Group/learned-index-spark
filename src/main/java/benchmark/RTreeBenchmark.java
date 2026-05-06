package benchmark;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import partitions.SpatialPartition;

public class RTreeBenchmark extends BenchmarkBase {

    @Override
    public String getPartitionName() {
        return "RTree";
    }

    @Override
    public JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception {
        return SpatialPartition.RtreePartitoner(pointRDD);
    }

    public static void main(String[] args) throws Exception {
        new RTreeBenchmark().run(args);
    }
}

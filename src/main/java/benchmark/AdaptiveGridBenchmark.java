package benchmark;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import partitions.SpatialPartition;

public class AdaptiveGridBenchmark extends BenchmarkBase {

    @Override
    public String getPartitionName() {
        return "AdaptiveGrid";
    }

    @Override
    public JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception {
        return SpatialPartition.AdaptiveGridPartitioner(pointRDD);
    }

    public static void main(String[] args) throws Exception {
        new AdaptiveGridBenchmark().run(args);
    }
}

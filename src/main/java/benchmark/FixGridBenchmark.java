package benchmark;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import partitions.SpatialPartition;

public class FixGridBenchmark extends BenchmarkBase {

    @Override
    public String getPartitionName() {
        return "FixGrid";
    }

    @Override
    public JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception {
        return SpatialPartition.FixGridPartitioner(pointRDD);
    }

    public static void main(String[] args) throws Exception {
        new FixGridBenchmark().run(args);
    }
}

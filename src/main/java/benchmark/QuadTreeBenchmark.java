package benchmark;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import partitions.SpatialPartition;

public class QuadTreeBenchmark extends BenchmarkBase {

    @Override
    public String getPartitionName() {
        return "QuadTree";
    }

    @Override
    public JavaRDD<Point> partition(JavaRDD<Point> pointRDD) throws Exception {
        return SpatialPartition.QuadtreePartitioner(pointRDD);
    }

    public static void main(String[] args) throws Exception {
        new QuadTreeBenchmark().run(args);
    }
}

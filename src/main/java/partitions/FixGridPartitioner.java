package partitions;


import datatypes.Point;
import datatypes.Rectangle;
import innerPartition.spatialPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;
import utils.StatCalculator;

import java.io.Serializable;
import java.util.Iterator;

public class FixGridPartitioner implements Serializable {
    int numPartitions;
    public JavaRDD<Point> rawSpatialRDD;
    public long approximateTotalCount = -1L;
    public Envelope boundaryEnvelope = null;
    public Rectangle boundingBox = null;
    private int sampleNumber = -1;
    public double GRID_STEP;
    private spatialPartitioner partitioner;

    public FixGridPartitioner(JavaRDD<Point> rawSpatialRDD){
        this.rawSpatialRDD = rawSpatialRDD;
    }
    public boolean analyze() {
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
        StatCalculator agg = (StatCalculator)this.rawSpatialRDD.aggregate((Object)null, seqOp, combOp);
        if (agg != null) {
            this.boundaryEnvelope = agg.getBoundary();
            this.approximateTotalCount = agg.getCount();
        } else {
            this.boundaryEnvelope = null;
            this.approximateTotalCount = 0L;
        }
        this.GRID_STEP = (boundaryEnvelope.getMaxX() - boundaryEnvelope.getMinX()) / 1000;
        return true;
    }

    public  JavaRDD<Point> partitionPoints( int numPartitions) {
        JavaPairRDD<Integer, Point> pairRDD = this.rawSpatialRDD.mapToPair(new PairFunction<Point, Integer, Point>() {
            @Override
            public Tuple2<Integer, Point> call(Point point) throws Exception {
                int partitionId = calculatePartitionId(point, boundaryEnvelope.getMinX(), GRID_STEP);
                return new Tuple2<>(partitionId, point);
            }
        });

        JavaPairRDD<Integer, Point> partitionedPairRDD = pairRDD.partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return numPartitions;
            }

            @Override
            public int getPartition(Object key) {
                return key.hashCode() % numPartitions();
            }
        });

        JavaRDD<Point> partitionRDD = partitionedPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Point>>, Point>() {
            @Override
            public Iterator<Point> call(Iterator<Tuple2<Integer, Point>> tuple2Iterator) throws Exception {
                return new Iterator<Point>() {
                    @Override
                    public boolean hasNext() {
                        return tuple2Iterator.hasNext();
                    }

                    @Override
                    public Point next() {
                        return tuple2Iterator.next()._2();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }, true);

        return partitionRDD;
    }
    // 你需要实现 calculatePartitionId 方法

    public static int calculatePartitionId(Point point, double boundingBoxX, double gridStep) {
        return (int) ((point.getX() - boundingBoxX) / gridStep);
    }
}

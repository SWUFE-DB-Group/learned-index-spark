package partitions;


import datatypes.Point;
import innerPartition.QuadPartition;
import innerPartition.spatialPartitioner;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Envelope;
import quadtree.QuadtreePartitioning;
import scala.Tuple2;
import utils.StatCalculator;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class QuadTreePartitioner implements Serializable {
    public JavaRDD<Point> rawSpatialRDD;
    public long approximateTotalCount = -1L;
    public Envelope boundaryEnvelope = null;
    private int sampleNumber = -1;
    private spatialPartitioner partitioner;

    public QuadTreePartitioner(JavaRDD<Point> rawSpatialRDD) {
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

        return true;
    }
    public JavaRDD<Point> partitionPoints(int numPartitions)throws Exception{
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount, this.sampleNumber);
        double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, this.approximateTotalCount, false);
        List<Envelope> samples = this.rawSpatialRDD.sample(false, fraction).map(new Function<Point, Envelope>() {
            public Envelope call(Point geometry) throws Exception {
                return geometry.getEnvelopeInternal();

            }
        }).collect();

        Envelope paddedBoundary = new Envelope(this.boundaryEnvelope.getMinX(), this.boundaryEnvelope.getMaxX() + 0.01D, this.boundaryEnvelope.getMinY(), this.boundaryEnvelope.getMaxY() + 0.01D);

        QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(samples, paddedBoundary, numPartitions);

        this.partitioner = new QuadPartition(quadtreePartitioning.getPartitionTree());

        JavaRDD<Point> spatialPartitionedRDD = this.partition(partitioner);

        return spatialPartitionedRDD;
    }

    private JavaRDD<Point> partition(final spatialPartitioner partitioner) {
        return this.rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Point, Integer, Point>() {
            public Iterator<Tuple2<Integer, Point>> call(Point spatialObject) throws Exception {
                return partitioner.placeObject(spatialObject);
            }
        }).partitionBy(partitioner).mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Point>>, Point>() {
            public Iterator<Point> call(final Iterator<Tuple2<Integer, Point>> tuple2Iterator) throws Exception {
                return new Iterator<Point>() {
                    public boolean hasNext() {
                        return tuple2Iterator.hasNext();
                    }

                    public Point next() {
                        return (Point) ((Tuple2)tuple2Iterator.next())._2();
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }, true);
    }



}

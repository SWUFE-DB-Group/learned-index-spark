package partitions;


import datatypes.Point;
import innerPartition.KDBPartitioner;
import innerPartition.spatialPartitioner;
import org.apache.sedona.core.spatialPartitioning.KDB;
import org.apache.sedona.core.spatialPartitioning.KDBTree;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;
import utils.StatCalculator;

import java.io.Serializable;
import java.util.*;

public class KDBTreePartitioner implements Serializable {
    int numPartitions;
    public JavaRDD<Point> rawSpatialRDD;
    public long approximateTotalCount = -1L;
    public Envelope boundaryEnvelope = null;
    private int sampleNumber = -1;
    private spatialPartitioner partitioner;
    private   KDBTree tree;
    public KDBTreePartitioner(JavaRDD<Point> rawSpatialRDD) {
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
                /*Envelope envelope= new Envelope(geometry.getX(),geometry.getX(),geometry.getY(),geometry.getY());
                return envelope;*/
                return geometry.getEnvelopeInternal();
            }
        }).collect();

        //System.out.println("+++++++++++++++++++++samples = "+samples.get(0));
        Envelope paddedBoundary = new Envelope(this.boundaryEnvelope.getMinX(), this.boundaryEnvelope.getMaxX() + 0.01D, this.boundaryEnvelope.getMinY(), this.boundaryEnvelope.getMaxY() + 0.01D);

        this.tree = new KDBTree(samples.size() / numPartitions, numPartitions, paddedBoundary);
        Iterator var9 = samples.iterator();

        while(var9.hasNext()) {
            Envelope sample = (Envelope)var9.next();
            tree.insert(sample);
        }

        tree.assignLeafIds();
        this.partitioner = new KDBPartitioner(tree);
        JavaRDD<Point> spatialPartitionedRDD = this.partition(partitioner);

        return  spatialPartitionedRDD;
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
    public  Iterator<Tuple2<Integer, Point>> placeObject(Point point) throws Exception {
        Objects.requireNonNull(point, "spatialObject");
        Envelope envelope = point.getEnvelopeInternal();
        List<KDBTree> matchedPartitions = this.tree.findLeafNodes(envelope);

        Set<Tuple2<Integer, Point>> result = new HashSet();
        Iterator var6 = matchedPartitions.iterator();

        while(true) {
            KDB leaf;
            do {
                if (!var6.hasNext()) {
                    return result.iterator();
                }

                leaf = (KDB)var6.next();
            } while(point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point.getX(),point.getY()));

            result.add(new Tuple2(leaf.getLeafId(), point));
        }
    }
}

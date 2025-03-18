package partitions;


import datatypes.Point;
import org.apache.sedona.core.spatialPartitioning.RtreePartitioning;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.random.SamplingUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;
import utils.StatCalculator;

import java.io.Serializable;
import java.util.List;

public class RtreePatitioner implements Serializable {
    public JavaRDD<Point> rawSpatialRDD;
    public long approximateTotalCount = -1L;
    public Envelope boundaryEnvelope = null;
    private int sampleNumber = -1;
    private SpatialPartitioner partitioner;

    public RtreePatitioner(JavaRDD<Point> rawSpatialRDD) {
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
                Envelope envelope = new Envelope(geometry.getX(),geometry.getX(),geometry.getY(),geometry.getY());
                return envelope;
                //return geometry.getEnvelopeInternal();
            }
        }).collect();
        RtreePartitioning partition = new RtreePartitioning(samples,numPartitions);
        List<Envelope> grids = partition.getGrids();

        JavaPairRDD<Integer, Point> pairRDD = rawSpatialRDD.mapToPair(new PairFunction<Point, Integer, Point>() {
            @Override
            public Tuple2<Integer, Point> call(Point point) throws Exception {
                Coordinate coordinate = new Coordinate(point.getX(),point.getY());
                for (int i = 0; i < grids.size(); i++) {
                    if (grids.get(i).contains(coordinate)) {
                        return new Tuple2<>(i, point);
                    }
                }
                return new Tuple2<>(0, point);
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
        JavaRDD<Point> resultRDD = partitionedPairRDD.map(new Function<Tuple2<Integer, Point>, Point>() {
            @Override
            public Point call(Tuple2<Integer, Point> tuple) throws Exception {
                return tuple._2(); // 返回值部分，即点数据
            }
        });
        return resultRDD;
    }


}

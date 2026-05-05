package partitions;

import datatypes.Point;
import diagnostics.PartitionDiagnostics;
import innerPartition.KDBPartitioner;
import innerPartition.spatialPartitioner;
import org.apache.sedona.core.spatialPartitioning.KDB;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;
import utils.StatCalculator;

import java.io.Serializable;
import java.util.*;

/**
 * KD-tree-based global spatial partitioner for LiLIS.
 * Fixed: KDBTree -> KDB (actual class name in sedona-core-3.0_2.12:1.2.0-incubating)
 * sampleRate default = 0.01 (1%) per paper spec.
 */
public class KDBTreePartitioner implements Serializable {

    private static final double DEFAULT_SAMPLE_RATE = 0.01D;

    int numPartitions;
    public JavaRDD<Point> rawSpatialRDD;
    public long approximateTotalCount = -1L;
    public Envelope boundaryEnvelope = null;
    private spatialPartitioner partitioner;
    private KDB tree;                          // KDB = KDBTree in Sedona 1.2.0
    private double sampleRate = DEFAULT_SAMPLE_RATE;

    public KDBTreePartitioner(JavaRDD<Point> rawSpatialRDD) {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    public KDBTreePartitioner(JavaRDD<Point> rawSpatialRDD, double sampleRate) {
        this.rawSpatialRDD = rawSpatialRDD;
        setSampleRate(sampleRate);
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(double sampleRate) {
        if (sampleRate <= 0.0D || sampleRate > 1.0D) {
            throw new IllegalArgumentException("sampleRate must be in (0, 1]. Got: " + sampleRate);
        }
        this.sampleRate = sampleRate;
    }

    public boolean analyze() {
        Function2<StatCalculator, StatCalculator, StatCalculator> combOp =
                (agg1, agg2) -> StatCalculator.combine(agg1, agg2);
        Function2<StatCalculator, Point, StatCalculator> seqOp =
                (agg, object) -> StatCalculator.add(agg, object);

        StatCalculator agg = (StatCalculator) this.rawSpatialRDD.aggregate(null, seqOp, combOp);
        if (agg != null) {
            this.boundaryEnvelope = agg.getBoundary();
            this.approximateTotalCount = agg.getCount();
        } else {
            this.boundaryEnvelope = null;
            this.approximateTotalCount = 0L;
        }
        return true;
    }

    public JavaRDD<Point> partitionPoints(int numPartitions) throws Exception {
        this.numPartitions = numPartitions;

        if (this.approximateTotalCount < 0L || this.boundaryEnvelope == null) {
            analyze();
        }

        if (this.approximateTotalCount <= 0L || this.boundaryEnvelope == null) {
            System.out.println("[KDBPartitioning] Empty input. Skip repartitioning.");
            return this.rawSpatialRDD;
        }

        // Sample 1% uniformly (paper spec)
        int targetSampleCount = (int) Math.max(1L,
                Math.round(this.approximateTotalCount * this.sampleRate));
        double fraction = Math.min(1.0D,
                (double) targetSampleCount / (double) this.approximateTotalCount);

        List<Envelope> samples = this.rawSpatialRDD
                .sample(false, fraction)
                .map((Function<Point, Envelope>) p -> p.getEnvelopeInternal())
                .collect();

        if (samples.isEmpty()) {
            List<Point> onePoint = this.rawSpatialRDD.take(1);
            if (!onePoint.isEmpty()) {
                samples.add(onePoint.get(0).getEnvelopeInternal());
            }
        }

        // Pad boundary slightly to avoid edge-point issues
        Envelope paddedBoundary = new Envelope(
                this.boundaryEnvelope.getMinX(),
                this.boundaryEnvelope.getMaxX() + 0.01D,
                this.boundaryEnvelope.getMinY(),
                this.boundaryEnvelope.getMaxY() + 0.01D);

        // Build KDB tree (= KDBTree in paper terminology)
        int maxItemsPerNode = Math.max(1, samples.size() / Math.max(1, numPartitions));
        this.tree = new KDB(maxItemsPerNode, numPartitions, paddedBoundary);

        for (Envelope sample : samples) {
            tree.insert(sample);
        }
        tree.assignLeafIds();

        this.partitioner = new KDBPartitioner(tree);
        JavaPairRDD<Integer, Point> partitionedPairs = this.partition(partitioner);
        JavaRDD<Point> spatialPartitionedRDD = this.valuesFromPartitionedPairs(partitionedPairs);

        long actualSampleCount = samples.size();
        double actualFraction = this.approximateTotalCount > 0L
                ? (double) actualSampleCount / (double) this.approximateTotalCount
                : 0.0D;

        // Log diagnostics
        System.out.printf(Locale.ROOT,
                "[KDBPartitioning] sampleRate=%.4f targetSample=%d actualSample=%d fraction=%.6f%n",
                this.sampleRate, targetSampleCount, actualSampleCount, actualFraction);

        // Write partition diagnostics
        try {
            String datasetLabel = this.rawSpatialRDD.name() != null
                    ? this.rawSpatialRDD.name() : "unnamed_rdd";
            PartitionDiagnostics.analyzeAndWrite(
                    this.rawSpatialRDD,
                    partitionedPairs,
                    this.partitioner.numPartitions(),
                    "results/phase2_partition_diagnostics.txt",
                    "KDBTreePartitioner",
                    datasetLabel,
                    this.sampleRate,
                    this.numPartitions,
                    actualSampleCount,
                    actualFraction);
        } catch (Exception e) {
            System.out.println("[KDBPartitioning] WARNING: diagnostics write failed: " + e.getMessage());
        }

        return spatialPartitionedRDD;
    }

    private JavaPairRDD<Integer, Point> partition(final spatialPartitioner partitioner) {
        return this.rawSpatialRDD
                .flatMapToPair(
                        (PairFlatMapFunction<Point, Integer, Point>) point ->
                                partitioner.placeObject(point))
                .partitionBy(partitioner);
    }

    private JavaRDD<Point> valuesFromPartitionedPairs(JavaPairRDD<Integer, Point> partitionedPairs) {
        return partitionedPairs.mapPartitions(
                (FlatMapFunction<Iterator<Tuple2<Integer, Point>>, Point>) tuple2Iterator ->
                        new Iterator<Point>() {
                            public boolean hasNext() { return tuple2Iterator.hasNext(); }
                            public Point next() { return tuple2Iterator.next()._2(); }
                            public void remove() { throw new UnsupportedOperationException(); }
                        },
                true);
    }

    public Iterator<Tuple2<Integer, Point>> placeObject(Point point) throws Exception {
        Objects.requireNonNull(point, "spatialObject");
        Envelope envelope = point.getEnvelopeInternal();
        List<KDB> matchedPartitions = this.tree.findLeafNodes(envelope);

        Set<Tuple2<Integer, Point>> result = new HashSet<>();
        for (KDB leaf : matchedPartitions) {
            if (new HalfOpenRectangle(leaf.getExtent()).contains(point.getX(), point.getY())) {
                result.add(new Tuple2<>(leaf.getLeafId(), point));
            }
        }
        return result.iterator();
    }
}
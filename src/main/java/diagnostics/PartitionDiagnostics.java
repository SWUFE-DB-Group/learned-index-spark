package diagnostics;

import datatypes.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class PartitionDiagnostics {

    private PartitionDiagnostics() {
    }

    public static Report analyzeAndWrite(
            JavaRDD<Point> beforeRDD,
            JavaPairRDD<Integer, Point> partitionedPairRDD,
            int configuredPartitionCount,
            String outputPath,
            String partitionerName,
            String datasetLabel,
            double sampleRate,
            int targetPartitions,
            long actualSampleCount,
            double actualSampleFraction) throws IOException {

        Report report = analyze(
            beforeRDD,
            partitionedPairRDD,
            configuredPartitionCount,
            partitionerName,
            datasetLabel,
            sampleRate,
            targetPartitions,
            actualSampleCount,
            actualSampleFraction
        );
        writeReport(report, outputPath);
        return report;
    }

    public static Report analyze(
            JavaRDD<Point> beforeRDD,
            JavaPairRDD<Integer, Point> partitionedPairRDD,
            int configuredPartitionCount,
            String partitionerName,
            String datasetLabel,
            double sampleRate,
            int targetPartitions,
            long actualSampleCount,
            double actualSampleFraction) {

        long countBefore = beforeRDD.count();
        long countAfter = partitionedPairRDD.count();

        int partitionCount = Math.max(1, configuredPartitionCount);

        Map<Integer, Long> perPartitionCounts = partitionedPairRDD
                .mapToPair(t -> new Tuple2<Integer, Long>(t._1, 1L))
                .reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long a, Long b) {
                        return a + b;
                    }
                })
                .collectAsMap();

        Map<Integer, EnvelopeAgg> perPartitionBbox = partitionedPairRDD
                .aggregateByKey(
                        new EnvelopeAgg(),
                        new Function2<EnvelopeAgg, Point, EnvelopeAgg>() {
                            @Override
                            public EnvelopeAgg call(EnvelopeAgg agg, Point point) {
                                agg.add(point.getX(), point.getY());
                                return agg;
                            }
                        },
                        new Function2<EnvelopeAgg, EnvelopeAgg, EnvelopeAgg>() {
                            @Override
                            public EnvelopeAgg call(EnvelopeAgg a, EnvelopeAgg b) {
                                return a.merge(b);
                            }
                        }
                )
                .collectAsMap();

        long minPoints = Long.MAX_VALUE;
        long maxPoints = Long.MIN_VALUE;
        long sumPoints = 0L;
        int emptyPartitionCount = 0;
        int nonEmptyPartitionsLE100 = 0;

        int bucket0 = 0;
        int bucket1To100 = 0;
        int bucket101To1000 = 0;
        int bucket1001To10000 = 0;
        int bucketGt10000 = 0;

        double minArea = Double.POSITIVE_INFINITY;
        double maxArea = Double.NEGATIVE_INFINITY;
        double sumArea = 0.0D;

        Map<Integer, Long> completePerPartitionCounts = new HashMap<Integer, Long>();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            long points = perPartitionCounts.containsKey(partitionId) ? perPartitionCounts.get(partitionId) : 0L;
            completePerPartitionCounts.put(partitionId, points);

            sumPoints += points;
            minPoints = Math.min(minPoints, points);
            maxPoints = Math.max(maxPoints, points);
            if (points == 0L) {
                emptyPartitionCount++;
            } else if (points <= 100L) {
                nonEmptyPartitionsLE100++;
            }

            if (points == 0L) {
                bucket0++;
            } else if (points <= 100L) {
                bucket1To100++;
            } else if (points <= 1000L) {
                bucket101To1000++;
            } else if (points <= 10000L) {
                bucket1001To10000++;
            } else {
                bucketGt10000++;
            }

            EnvelopeAgg agg = perPartitionBbox.get(partitionId);
            double area = agg == null ? 0.0D : agg.area();
            sumArea += area;
            minArea = Math.min(minArea, area);
            maxArea = Math.max(maxArea, area);
        }

        double avgPoints = partitionCount == 0 ? 0.0D : (double) sumPoints / (double) partitionCount;
        double avgArea = partitionCount == 0 ? 0.0D : sumArea / (double) partitionCount;
    int nonEmptyPartitionCount = partitionCount - emptyPartitionCount;
    double fallbackRatioOnNonEmpty = nonEmptyPartitionCount == 0
        ? 0.0D
        : (double) nonEmptyPartitionsLE100 / (double) nonEmptyPartitionCount;

        return new Report(
                partitionerName,
        datasetLabel,
        sampleRate,
        targetPartitions,
        actualSampleCount,
        actualSampleFraction,
                countBefore,
                countAfter,
                partitionCount,
                minPoints == Long.MAX_VALUE ? 0L : minPoints,
                avgPoints,
                maxPoints == Long.MIN_VALUE ? 0L : maxPoints,
        emptyPartitionCount,
        nonEmptyPartitionsLE100,
        fallbackRatioOnNonEmpty,
        bucket0,
        bucket1To100,
        bucket101To1000,
        bucket1001To10000,
        bucketGt10000,
                minArea == Double.POSITIVE_INFINITY ? 0.0D : minArea,
                avgArea,
                maxArea == Double.NEGATIVE_INFINITY ? 0.0D : maxArea,
        new HashMap<Integer, Long>(completePerPartitionCounts)
        );
    }

    public static void writeReport(Report report, String outputPath) throws IOException {
        File outFile = new File(outputPath);
        File parent = outFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile, true))) {
            writer.write("========================================");
            writer.newLine();
            writer.write("timestamp=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).format(new Date()));
            writer.newLine();
            writer.write("partitioner=" + report.partitionerName);
            writer.newLine();
            writer.write("datasetLabel=" + report.datasetLabel);
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "sampleRate=%.6f", report.sampleRate));
            writer.newLine();
            writer.write("targetPartitions=" + report.targetPartitions);
            writer.newLine();
            writer.write("actualSampleCount=" + report.actualSampleCount);
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "actualSampleFraction=%.6f", report.actualSampleFraction));
            writer.newLine();
            writer.write("countBefore=" + report.countBefore);
            writer.newLine();
            writer.write("countAfter=" + report.countAfter);
            writer.newLine();
            writer.write("partitionCount=" + report.partitionCount);
            writer.newLine();
            writer.write("minPointsPerPartition=" + report.minPointsPerPartition);
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "avgPointsPerPartition=%.4f", report.avgPointsPerPartition));
            writer.newLine();
            writer.write("maxPointsPerPartition=" + report.maxPointsPerPartition);
            writer.newLine();
            writer.write("emptyPartitionCount=" + report.emptyPartitionCount);
            writer.newLine();
            writer.write("nonEmptyPartitionsLE100=" + report.nonEmptyPartitionsLE100);
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "fallbackRatioOnNonEmptyPartitions=%.6f", report.fallbackRatioOnNonEmptyPartitions));
            writer.newLine();
            writer.write("histogram.0=" + report.histogram0);
            writer.newLine();
            writer.write("histogram.1-100=" + report.histogram1To100);
            writer.newLine();
            writer.write("histogram.101-1000=" + report.histogram101To1000);
            writer.newLine();
            writer.write("histogram.1001-10000=" + report.histogram1001To10000);
            writer.newLine();
            writer.write("histogram.>10000=" + report.histogramGt10000);
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "minBBoxAreaPerPartition=%.6f", report.minBBoxAreaPerPartition));
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "avgBBoxAreaPerPartition=%.6f", report.avgBBoxAreaPerPartition));
            writer.newLine();
            writer.write(String.format(Locale.ROOT, "maxBBoxAreaPerPartition=%.6f", report.maxBBoxAreaPerPartition));
            writer.newLine();

            writer.write("perPartitionCounts:");
            writer.newLine();
            java.util.List<Integer> partitionIds = new java.util.ArrayList<Integer>(report.perPartitionCounts.keySet());
            java.util.Collections.sort(partitionIds);
            for (Integer partitionId : partitionIds) {
                writer.write("  p" + partitionId + "=" + report.perPartitionCounts.get(partitionId));
                writer.newLine();
            }
        }
    }

    private static final class EnvelopeAgg implements Serializable {
        private double minX = Double.POSITIVE_INFINITY;
        private double maxX = Double.NEGATIVE_INFINITY;
        private double minY = Double.POSITIVE_INFINITY;
        private double maxY = Double.NEGATIVE_INFINITY;
        private boolean initialized = false;

        void add(double x, double y) {
            if (!initialized) {
                minX = x;
                maxX = x;
                minY = y;
                maxY = y;
                initialized = true;
                return;
            }
            minX = Math.min(minX, x);
            maxX = Math.max(maxX, x);
            minY = Math.min(minY, y);
            maxY = Math.max(maxY, y);
        }

        EnvelopeAgg merge(EnvelopeAgg other) {
            if (other == null || !other.initialized) {
                return this;
            }
            if (!this.initialized) {
                this.minX = other.minX;
                this.maxX = other.maxX;
                this.minY = other.minY;
                this.maxY = other.maxY;
                this.initialized = true;
                return this;
            }

            this.minX = Math.min(this.minX, other.minX);
            this.maxX = Math.max(this.maxX, other.maxX);
            this.minY = Math.min(this.minY, other.minY);
            this.maxY = Math.max(this.maxY, other.maxY);
            return this;
        }

        double area() {
            if (!initialized) {
                return 0.0D;
            }
            return Math.max(0.0D, (maxX - minX) * (maxY - minY));
        }
    }

    public static final class Report implements Serializable {
        public final String partitionerName;
        public final String datasetLabel;
        public final double sampleRate;
        public final int targetPartitions;
        public final long actualSampleCount;
        public final double actualSampleFraction;
        public final long countBefore;
        public final long countAfter;
        public final int partitionCount;
        public final long minPointsPerPartition;
        public final double avgPointsPerPartition;
        public final long maxPointsPerPartition;
        public final int emptyPartitionCount;
        public final int nonEmptyPartitionsLE100;
        public final double fallbackRatioOnNonEmptyPartitions;
        public final int histogram0;
        public final int histogram1To100;
        public final int histogram101To1000;
        public final int histogram1001To10000;
        public final int histogramGt10000;
        public final double minBBoxAreaPerPartition;
        public final double avgBBoxAreaPerPartition;
        public final double maxBBoxAreaPerPartition;
        public final Map<Integer, Long> perPartitionCounts;

        public Report(
                String partitionerName,
                String datasetLabel,
                double sampleRate,
                int targetPartitions,
                long actualSampleCount,
                double actualSampleFraction,
                long countBefore,
                long countAfter,
                int partitionCount,
                long minPointsPerPartition,
                double avgPointsPerPartition,
                long maxPointsPerPartition,
                int emptyPartitionCount,
                int nonEmptyPartitionsLE100,
                double fallbackRatioOnNonEmptyPartitions,
                int histogram0,
                int histogram1To100,
                int histogram101To1000,
                int histogram1001To10000,
                int histogramGt10000,
                double minBBoxAreaPerPartition,
                double avgBBoxAreaPerPartition,
                double maxBBoxAreaPerPartition,
                Map<Integer, Long> perPartitionCounts) {
            this.partitionerName = partitionerName;
            this.datasetLabel = datasetLabel;
            this.sampleRate = sampleRate;
            this.targetPartitions = targetPartitions;
            this.actualSampleCount = actualSampleCount;
            this.actualSampleFraction = actualSampleFraction;
            this.countBefore = countBefore;
            this.countAfter = countAfter;
            this.partitionCount = partitionCount;
            this.minPointsPerPartition = minPointsPerPartition;
            this.avgPointsPerPartition = avgPointsPerPartition;
            this.maxPointsPerPartition = maxPointsPerPartition;
            this.emptyPartitionCount = emptyPartitionCount;
            this.nonEmptyPartitionsLE100 = nonEmptyPartitionsLE100;
            this.fallbackRatioOnNonEmptyPartitions = fallbackRatioOnNonEmptyPartitions;
            this.histogram0 = histogram0;
            this.histogram1To100 = histogram1To100;
            this.histogram101To1000 = histogram101To1000;
            this.histogram1001To10000 = histogram1001To10000;
            this.histogramGt10000 = histogramGt10000;
            this.minBBoxAreaPerPartition = minBBoxAreaPerPartition;
            this.avgBBoxAreaPerPartition = avgBBoxAreaPerPartition;
            this.maxBBoxAreaPerPartition = maxBBoxAreaPerPartition;
            this.perPartitionCounts = perPartitionCounts;
        }
    }
}

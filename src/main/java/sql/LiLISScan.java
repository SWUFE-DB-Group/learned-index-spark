package sql;

import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import query.RangeQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LiLISScan implements Scan, Batch {
    private final String indexName;
    private final LiLISRange range;
    private final IndexedSpatialRDD index;

    public LiLISScan(String indexName, LiLISRange range) {
        this.indexName = indexName;
        this.range = range;
        this.index = LiLISIndexRegistry.get(indexName);
    }

    @Override
    public StructType readSchema() {
        return LiLISTable.SCHEMA;
    }

    @Override
    public String description() {
        return range != null && range.isComplete()
                ? "LiLIS index scan with pushed range: " + indexName
                : "LiLIS index scan: " + indexName;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<Point> points = readPoints();
        if (points.isEmpty()) {
            return new InputPartition[]{new LiLISInputPartition(Collections.emptyList())};
        }
        int partitions = Math.max(1, Math.min(points.size(), index.getIndexRDD().getNumPartitions()));
        List<InputPartition> inputPartitions = new ArrayList<>();
        int chunkSize = (int) Math.ceil(points.size() / (double) partitions);
        for (int start = 0; start < points.size(); start += chunkSize) {
            int end = Math.min(points.size(), start + chunkSize);
            inputPartitions.add(new LiLISInputPartition(new ArrayList<>(points.subList(start, end))));
        }
        return inputPartitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new LiLISPartitionReaderFactory();
    }

    private List<Point> readPoints() {
        Rectangle queryRange = range != null && range.isComplete() ? range.toRectangle() : index.getBoundingBox();
        List<Point> points = RangeQuery.SpatialRangeQuery(index.getIndexRDD(), queryRange).collect();
        if (range == null || range.isComplete()) {
            return points;
        }
        List<Point> filtered = new ArrayList<>();
        for (Point point : points) {
            if (range.contains(point.getX(), point.getY())) {
                filtered.add(point);
            }
        }
        return filtered;
    }
}

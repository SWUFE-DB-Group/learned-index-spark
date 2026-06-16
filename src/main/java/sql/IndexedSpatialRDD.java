package sql;

import datatypes.Rectangle;
import org.apache.spark.api.java.JavaRDD;
import spline.Spline;

import java.io.Serializable;

public class IndexedSpatialRDD implements Serializable {
    private final JavaRDD<Spline> indexRDD;
    private final Rectangle boundingBox;
    private final long totalCount;
    private final String partitionMethod;

    public IndexedSpatialRDD(JavaRDD<Spline> indexRDD, Rectangle boundingBox, long totalCount, String partitionMethod) {
        this.indexRDD = indexRDD;
        this.boundingBox = boundingBox;
        this.totalCount = totalCount;
        this.partitionMethod = partitionMethod;
    }

    public JavaRDD<Spline> getIndexRDD() {
        return indexRDD;
    }

    public Rectangle getBoundingBox() {
        return boundingBox;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public String getPartitionMethod() {
        return partitionMethod;
    }
}

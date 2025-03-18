package innerPartition;

import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.spark.Partitioner;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public abstract class spatialPartitioner extends Partitioner implements Serializable {
    protected final List<Envelope> grids;

    protected spatialPartitioner(List<Envelope> grids) {
        this.grids = grids;
    }
    public abstract  Iterator<Tuple2<Integer, Point>> placeObject(Point var1) throws Exception;

    @Nullable
    public abstract DedupParams getDedupParams();

    public List<Envelope> getGrids() {
        return this.grids;
    }

    public int getPartition(Object key) {
        return (Integer)key;
    }
}

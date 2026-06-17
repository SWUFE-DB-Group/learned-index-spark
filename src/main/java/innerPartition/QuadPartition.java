package innerPartition;

import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import quadtree.QuadRectangle;
import quadtree.QuadTree;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Quad-tree partitioner for LiLIS.
 * Fixed: QuadTreePartitioner import path corrected to
 *   org.apache.sedona.core.spatialPartitioning.QuadTreePartitioner
 *   (not the quadtree subpackage — that class doesn't exist in 1.2.0-incubating JAR)
 */
public class QuadPartition extends spatialPartitioner {

    private final QuadTree quadTree;

    public QuadPartition(QuadTree quadTree) {
        super(getLeafGrids(quadTree));
        this.quadTree = quadTree;
        this.quadTree.dropElements();
    }

    private static List<Envelope> getLeafGrids(QuadTree quadTree) {
        List<QuadRectangle> zones = quadTree.getLeafZones();
        List<Envelope> grids = new ArrayList<>();
        for (QuadRectangle zone : zones) {
            grids.add(zone.getEnvelope());
        }
        return grids;
    }

    @Override
    public Iterator<Tuple2<Integer, Point>> placeObject(Point spatialObject) throws Exception {
        Envelope envelope = spatialObject.getEnvelopeInternal();
        List<QuadRectangle> matchedPartitions =
                this.quadTree.findZones(new QuadRectangle(envelope));

        Point point = spatialObject instanceof Point ? spatialObject : null;
        Set<Tuple2<Integer, Point>> result = new HashSet<>();

        for (QuadRectangle rectangle : matchedPartitions) {
            if (point == null ||
                    new HalfOpenRectangle(rectangle.getEnvelope()).contains(point.getX(), point.getY())) {
                result.add(new Tuple2<>(rectangle.partitionId, spatialObject));
            }
        }
        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        return new DedupParams(this.grids);
    }

    @Override
    public int numPartitions() {
        return this.grids.size();
    }
}
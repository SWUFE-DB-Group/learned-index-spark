package innerPartition;


import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadTreePartitioner;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import quadtree.QuadRectangle;
import quadtree.QuadTree;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class QuadPartition extends spatialPartitioner {

    private final QuadTree quadTree;

    public QuadPartition(QuadTree quadTree) {
        super(getLeafGrids(quadTree));
        this.quadTree = quadTree;
        this.quadTree.dropElements();
    }

    private static List<Envelope> getLeafGrids(QuadTree quadTree) {
        List<QuadRectangle> zones = quadTree.getLeafZones();
        List<Envelope> grids = new ArrayList();
        Iterator var3 = zones.iterator();

        while(var3.hasNext()) {
            QuadRectangle zone = (QuadRectangle)var3.next();
            grids.add(zone.getEnvelope());
        }

        return grids;
    }



    @Override
    public Iterator<Tuple2<Integer, Point>> placeObject(Point spatialObject) throws Exception {
        Envelope envelope = spatialObject.getEnvelopeInternal();
        List<org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle> matchedPartitions = this.quadTree.findZones(new QuadRectangle(envelope));
        Point point = spatialObject instanceof Point ? spatialObject : null;
        Set<Tuple2<Integer, Point>> result = new HashSet();
        Iterator var6 = matchedPartitions.iterator();

        while(true) {
            QuadRectangle rectangle;
            do {
                if (!var6.hasNext()) {
                    return result.iterator();
                }

                rectangle = (QuadRectangle)var6.next();
            } while(point != null && !(new HalfOpenRectangle(rectangle.getEnvelope())).contains(point.getX(),point.getY()));

            result.add(new Tuple2(rectangle.partitionId, spatialObject));
        }
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

    public boolean equals(Object o) {
        if (o != null && o instanceof QuadTreePartitioner) {
            QuadPartition other = (QuadPartition)o;
            return other.quadTree.equals(this.quadTree);
        } else {
            return false;
        }
    }
}

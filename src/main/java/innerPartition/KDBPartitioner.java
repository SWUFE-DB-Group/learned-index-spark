package innerPartition;

import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.KDB;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class KDBPartitioner extends spatialPartitioner {

    private final KDB tree;

    public KDBPartitioner(KDB tree) {
        super(getLeafZones(tree));
        this.tree = tree;
        this.tree.dropElements();
    }

    private static List<Envelope> getLeafZones(KDB tree) {
        final List<Envelope> leafs = new ArrayList();
        tree.traverse(new KDB.Visitor() {
            public boolean visit(KDB tree) {
                if (tree.isLeaf()) {
                    leafs.add(tree.getExtent());
                }

                return true;
            }
        });
        return leafs;
    }

    @Override
    public Iterator<Tuple2<Integer, Point>> placeObject(Point spatialObject) throws Exception {
        Envelope envelope = spatialObject.getEnvelopeInternal();
        List<KDB> matchedPartitions = this.tree.findLeafNodes(envelope);
        Point point = spatialObject instanceof Point ? spatialObject : null;
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

            result.add(new Tuple2(leaf.getLeafId(), spatialObject));
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
}

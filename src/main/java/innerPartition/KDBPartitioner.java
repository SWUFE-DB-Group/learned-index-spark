package innerPartition;

import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.KDBTree;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class KDBPartitioner extends spatialPartitioner {

    private final KDBTree tree;

    public KDBPartitioner(KDBTree tree) {
        super(getLeafZones(tree));
        this.tree = tree;
        this.tree.dropElements();
    }

    private static List<Envelope> getLeafZones(KDBTree tree) {
        final List<Envelope> leafs = new ArrayList();
        tree.traverse(new KDBTree.Visitor() {
            public boolean visit(KDBTree tree) {
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
        List<KDBTree> matchedPartitions = this.tree.findLeafNodes(envelope);
        Point point = spatialObject instanceof Point ? spatialObject : null;
        Set<Tuple2<Integer, Point>> result = new HashSet();
        Iterator var6 = matchedPartitions.iterator();

        while(true) {
            KDBTree leaf;
            do {
                if (!var6.hasNext()) {
                    return result.iterator();
                }

                leaf = (KDBTree)var6.next();
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

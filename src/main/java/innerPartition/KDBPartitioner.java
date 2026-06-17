package innerPartition;

import datatypes.Point;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.KDB;
import org.apache.sedona.core.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

/**
 * KDB (KD-tree-based) partitioner for LiLIS.
 * Uses KDB (= KDBTree in Sedona 1.2.0 JAR) as the spatial index.
 * Fixed: KDBTree -> KDB (actual class name in sedona-core-3.0_2.12:1.2.0-incubating)
 */
public class KDBPartitioner extends spatialPartitioner {

    private final KDB tree;

    public KDBPartitioner(KDB tree) {
        super(getLeafZones(tree));
        this.tree = tree;
        this.tree.dropElements();
    }

    private static List<Envelope> getLeafZones(KDB tree) {
        final List<Envelope> leafs = new ArrayList<>();
        tree.traverse(new KDB.Visitor() {
            public boolean visit(KDB node) {
                if (node.isLeaf()) {
                    leafs.add(node.getExtent());
                }
                return true;
            }
        });
        return leafs;
    }

    @Override
    public Iterator<Tuple2<Integer, Point>> placeObject(Point spatialObject) throws Exception {
        Objects.requireNonNull(spatialObject, "spatialObject");
        Envelope envelope = spatialObject.getEnvelopeInternal();
        List<KDB> matchedPartitions = this.tree.findLeafNodes(envelope);

        Point point = spatialObject instanceof Point ? spatialObject : null;
        Set<Tuple2<Integer, Point>> result = new HashSet<>();
        Iterator<KDB> var6 = matchedPartitions.iterator();

        while (true) {
            KDB leaf;
            do {
                if (!var6.hasNext()) {
                    return result.iterator();
                }
                leaf = var6.next();
            } while (point != null &&
                    !(new HalfOpenRectangle(leaf.getExtent())).contains(point.getX(), point.getY()));

            result.add(new Tuple2<>(leaf.getLeafId(), spatialObject));
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
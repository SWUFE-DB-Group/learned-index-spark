package quadtree;



import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class QuadtreePartitioning implements Serializable {
    private final QuadTree<Integer> partitionTree;


    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, int partitions) throws Exception {
        this(samples, boundary, partitions, -1);
    }
    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, int partitions, int minTreeLevel) throws Exception {
        int maxItemsPerNode = samples.size() / partitions;
        this.partitionTree = new QuadTree(new QuadRectangle(boundary), 0, maxItemsPerNode, partitions);
        if (minTreeLevel > 0) {
            this.partitionTree.forceGrowUp(minTreeLevel);
        }

        Iterator var7 = samples.iterator();

        while(var7.hasNext()) {
            Envelope sample = (Envelope)var7.next();
            this.partitionTree.insert(new QuadRectangle(sample), 1);
        }

        this.partitionTree.assignPartitionIds();
    }

    public QuadTree getPartitionTree() {
        return this.partitionTree;
    }
}

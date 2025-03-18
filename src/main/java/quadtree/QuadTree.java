package quadtree;

import org.apache.commons.lang3.mutable.MutableInt;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QuadTree<T> implements Serializable {
    public static final int REGION_SELF = -1;
    public static final int REGION_NW = 0;
    public static final int REGION_NE = 1;
    public static final int REGION_SW = 2;
    public static final int REGION_SE = 3;
    private final int maxItemsPerZone;
    private final int maxLevel;
    private final int level;
    private final List<QuadNode<T>> nodes;
    private final QuadRectangle zone;
    private int nodeNum;
    private QuadTree<T>[] regions;
    public QuadTree(QuadRectangle definition, int level) {
        this(definition, level, 5, 10);
    }

    public QuadTree(QuadRectangle definition, int level, int maxItemsPerZone, int maxLevel) {
        this.nodes = new ArrayList();
        this.nodeNum = 0;
        this.maxItemsPerZone = maxItemsPerZone;
        this.maxLevel = maxLevel;
        this.zone = definition;
        this.level = level;
    }
    public QuadRectangle getZone() {
        return this.zone;
    }

    private int findRegion(QuadRectangle r, boolean split) {
        int region = -1;
        if (this.nodeNum >= this.maxItemsPerZone && this.level < this.maxLevel) {
            if (this.regions == null && split) {
                this.split();
            }

            if (this.regions != null) {
                for(int i = 0; i < this.regions.length; ++i) {
                    if (this.regions[i].getZone().contains(r)) {
                        region = i;
                        break;
                    }
                }
            }
        }

        return region;
    }
    private int findRegion(int x, int y) {
        int region = -1;
        if (this.regions != null) {
            for(int i = 0; i < this.regions.length; ++i) {
                if (this.regions[i].getZone().contains((double)x, (double)y)) {
                    region = i;
                    break;
                }
            }
        }

        return region;
    }
    private QuadTree<T> newQuadTree(QuadRectangle zone, int level) {
        return new QuadTree(zone, level, this.maxItemsPerZone, this.maxLevel);
    }

    private void split() {
        this.regions = new QuadTree[4];
        double newWidth = this.zone.width / 2.0D;
        double newHeight = this.zone.height / 2.0D;
        int newLevel = this.level + 1;
        this.regions[0] = this.newQuadTree(new QuadRectangle(this.zone.x, this.zone.y + this.zone.height / 2.0D, newWidth, newHeight), newLevel);
        this.regions[1] = this.newQuadTree(new QuadRectangle(this.zone.x + this.zone.width / 2.0D, this.zone.y + this.zone.height / 2.0D, newWidth, newHeight), newLevel);
        this.regions[2] = this.newQuadTree(new QuadRectangle(this.zone.x, this.zone.y, newWidth, newHeight), newLevel);
        this.regions[3] = this.newQuadTree(new QuadRectangle(this.zone.x + this.zone.width / 2.0D, this.zone.y, newWidth, newHeight), newLevel);
    }
    public void forceGrowUp(int minLevel) {
        if (minLevel < 1) {
            throw new IllegalArgumentException("minLevel must be >= 1. Received " + minLevel);
        } else {
            this.split();
            this.nodeNum = this.maxItemsPerZone;
            if (this.level + 1 < minLevel) {
                QuadTree[] var2 = this.regions;
                int var3 = var2.length;

                for(int var4 = 0; var4 < var3; ++var4) {
                    QuadTree<T> region = var2[var4];
                    region.forceGrowUp(minLevel);
                }

            }
        }
    }

    public void insert(QuadRectangle r, T element) {
        int region = this.findRegion(r, true);
        if (region != -1 && this.level != this.maxLevel) {
            this.regions[region].insert(r, element);
            if (this.nodeNum >= this.maxItemsPerZone && this.level < this.maxLevel) {
                List<QuadNode<T>> tempNodes = new ArrayList();
                tempNodes.addAll(this.nodes);
                this.nodes.clear();
                Iterator var5 = tempNodes.iterator();

                while(var5.hasNext()) {
                    QuadNode<T> node = (QuadNode<T>)var5.next();
                    this.insert(node.r, node.element);
                }
            }

        } else {
            this.nodes.add(new QuadNode(r, element));
            ++this.nodeNum;
        }
    }
    public void dropElements() {
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                tree.nodes.clear();
                return true;
            }
        });
    }
    public List<T> getElements(QuadRectangle r) {
        int region = this.findRegion(r, false);
        List<T> list = new ArrayList();
        if (region != -1) {
            Iterator var4 = this.nodes.iterator();

            while(var4.hasNext()) {
                QuadNode<T> node = (QuadNode)var4.next();
                list.add(node.element);
            }

            list.addAll(this.regions[region].getElements(r));
        } else {
            this.addAllElements(list);
        }

        return list;
    }

    private void traverse(Visitor<T> visitor) {
        if (visitor.visit(this)) {
            if (this.regions != null) {
                this.regions[0].traverse(visitor);
                this.regions[1].traverse(visitor);
                this.regions[2].traverse(visitor);
                this.regions[3].traverse(visitor);
            }

        }
    }
    private void traverseWithTrace(VisitorWithLineage<T> visitor, String lineage) {
        if (visitor.visit(this, lineage)) {
            if (this.regions != null) {
                this.regions[0].traverseWithTrace(visitor, lineage + 0);
                this.regions[1].traverseWithTrace(visitor, lineage + 1);
                this.regions[2].traverseWithTrace(visitor, lineage + 2);
                this.regions[3].traverseWithTrace(visitor, lineage + 3);
            }

        }
    }
    private void addAllElements(final List<T> list) {
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                Iterator var2 = tree.nodes.iterator();

                while(var2.hasNext()) {
                    QuadNode<T> node = (QuadNode)var2.next();
                    list.add(node.element);
                }

                return true;
            }
        });
    }

    public boolean isLeaf() {
        return this.regions == null;
    }

    public List<QuadRectangle> getAllZones() {
        final List<QuadRectangle> zones = new ArrayList();
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                zones.add(tree.zone);
                return true;
            }
        });
        return zones;
    }
    public List<QuadRectangle> getLeafZones() {
        final List<QuadRectangle> leafZones = new ArrayList();
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafZones.add(tree.zone);
                }

                return true;
            }
        });
        return leafZones;
    }
    public int getTotalNumLeafNode() {
        final MutableInt leafCount = new MutableInt(0);
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                if (tree.isLeaf()) {
                    leafCount.increment();
                }

                return true;
            }
        });
        return leafCount.getValue();
    }

    public QuadRectangle getZone(int x, int y) throws ArrayIndexOutOfBoundsException {
        int region = this.findRegion(x, y);
        if (region != -1) {
            return this.regions[region].getZone(x, y);
        } else if (this.zone.contains((double)x, (double)y)) {
            return this.zone;
        } else {
            throw new ArrayIndexOutOfBoundsException("[Sedona][StandardQuadTree] this pixel is out of the quad tree boundary.");
        }
    }
    public QuadRectangle getParentZone(int x, int y, int minLevel) throws Exception {
        int region = this.findRegion(x, y);
        if (this.level < minLevel) {
            if (region == -1) {
                assert this.regions == null;

                if (this.zone.contains((double)x, (double)y)) {
                    throw new Exception("[Sedona][StandardQuadTree][getParentZone] this leaf node doesn't have enough depth. Please check ForceGrowUp. Expected: " + minLevel + " Actual: " + this.level + ". Query point: " + x + " " + y + ". Tree statistics, total leaf nodes: " + this.getTotalNumLeafNode());
                } else {
                    throw new Exception("[Sedona][StandardQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
                }
            } else {
                return this.regions[region].getParentZone(x, y, minLevel);
            }
        } else if (this.zone.contains((double)x, (double)y)) {
            return this.zone;
        } else {
            throw new Exception("[Sedona][StandardQuadTree][getParentZone] this pixel is out of the quad tree boundary.");
        }
    }

    public List<QuadRectangle> findZones(QuadRectangle r) {
        final Envelope envelope = r.getEnvelope();
        final List<QuadRectangle> matches = new ArrayList();
        this.traverse(new Visitor<T>() {
            public boolean visit(QuadTree<T> tree) {
                if (!QuadTree.this.disjoint(tree.zone.getEnvelope(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree.zone);
                    }

                    return true;
                } else {
                    return false;
                }
            }
        });
        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2) {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }
    public void assignPartitionIds() {
        this.traverse(new Visitor<T>() {
            private int partitionId = 0;

            public boolean visit(QuadTree<T> tree) {
                if (tree.isLeaf()) {
                    tree.getZone().partitionId = this.partitionId;
                    ++this.partitionId;
                }

                return true;
            }
        });
    }
    public void assignPartitionLineage() {
        this.traverseWithTrace(new VisitorWithLineage<T>() {
            public boolean visit(QuadTree<T> tree, String lineage) {
                if (tree.isLeaf()) {
                    tree.getZone().lineage = lineage;
                }

                return true;
            }
        }, "");
    }


    private interface VisitorWithLineage<T>{
        boolean visit(QuadTree<T> var1, String var2);
    }

    private interface Visitor<T> {
        boolean visit(QuadTree<T> var1);
    }

}

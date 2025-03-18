package quadtree;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;

public class QuadRectangle implements Serializable {
    public final double x;
    public final double y;
    public final double width;
    public final double height;
    public Integer partitionId = null;
    public String lineage = null;

    public QuadRectangle(Envelope envelope) {
        this.x = envelope.getMinX();
        this.y = envelope.getMinY();
        this.width = envelope.getWidth();
        this.height = envelope.getHeight();
    }

    public QuadRectangle(double x, double y, double width, double height) {
        if (width < 0.0D) {
            throw new IllegalArgumentException("width must be >= 0");
        } else if (height < 0.0D) {
            throw new IllegalArgumentException("height must be >= 0");
        } else {
            this.x = x;
            this.y = y;
            this.width = width;
            this.height = height;
        }
    }

    public boolean contains(double x, double y) {
        return x >= this.x && x <= this.x + this.width && y >= this.y && y <= this.y + this.height;
    }

    public boolean contains(QuadRectangle r) {
        return r.x >= this.x && r.x + r.width <= this.x + this.width && r.y >= this.y && r.y + r.height <= this.y + this.height;
    }

    public int getUniqueId() {
        return this.hashCode();
    }

    public Envelope getEnvelope() {
        return new Envelope(this.x, this.x + this.width, this.y, this.y + this.height);
    }

    public String toString() {
        return "x: " + this.x + " y: " + this.y + " w: " + this.width + " h: " + this.height + " PartitionId: " + this.partitionId + " Lineage: " + this.lineage;
    }

    public boolean equals(Object o) {
        if (o != null && o instanceof QuadRectangle) {
            QuadRectangle other = (QuadRectangle)o;
            return this.x == other.x && this.y == other.y && this.width == other.width && this.height == other.height && this.partitionId == other.partitionId;
        } else {
            return false;
        }
    }

    public int hashCode() {
        String stringId = "" + this.x + this.y + this.width + this.height;
        return stringId.hashCode();
    }
}
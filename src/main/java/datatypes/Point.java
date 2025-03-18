package datatypes;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.Objects;

public class Point implements Serializable {
    private double x; // lat
    private double y; // lon

    public Point(){}

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }
    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public boolean equals(Point other) {
        return x == other.getX() && y == other.getY();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Point other = (Point) obj;
        return equals(other);
    }
    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
    public boolean lessThan(Point other) {
        return x == other.getX() ? y < other.getY() : x < other.getX();
    }

    public void add(Point other) {
        x += other.getX();
        y += other.getY();
    }

    public void subtract(Point other) {
        x -= other.getX();
        y -= other.getY();
    }

    public void assign(Point other) {
        x = other.getX();
        y = other.getY();
    }
    public Envelope getEnvelopeInternal(){
        Envelope envelope = new Envelope(x,x,y,y);
        return envelope;
    }

    @Override
    public String toString() {
        return "(" + x + "|" + y + ")";
    }
    public double euclideanLength() {
        return euclideanDistance(new Point(0, 0));
    }
    public double euclideanDistance(Point other) {
        return Math.sqrt(euclideanDistanceSquare(other));
    }
    public double euclideanDistanceSquare(Point other) {
        double dx = x - other.getX();
        double dy = y - other.getY();
        return dx * dx + dy * dy;
    }

    public double distanceTo(Point other) {
        return Math.sqrt((this.x - other.x) * (this.x - other.x) + (this.y - other.y) * (this.y - other.y));
    }


}

package datatypes;




import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.Objects;

public class Rectangle implements Serializable {
    private Point from;
    private Point to;

    public Rectangle(){

    }


    public Rectangle(Point from, Point to) {
        this.from = from;
        this.to = to;
    }
    public Rectangle(double fromX, double fromY, double toX, double toY) {

        /*Coordinate coordinateP = new Coordinate(fromX, fromY);
        Coordinate coordinatetoP = new Coordinate(toX, toY);
        GeometryFactory geometryFactory = new GeometryFactory();
        Point fromP = geometryFactory.createPoint(coordinateP);
        Point toP = geometryFactory.createPoint(coordinatetoP);*/
        Point fromP = new Point(fromX,fromY);
        Point toP = new Point(toX,toY);
        this.from = fromP;
        this.to = toP;
    }

    public Rectangle(Envelope envelope) {
        /*Coordinate coordinateP = new Coordinate(envelope.getMinX(),envelope.getMinY());
        Coordinate coordinatetoP = new Coordinate(envelope.getMaxX(),envelope.getMaxY());
        GeometryFactory geometryFactory = new GeometryFactory();

        Point fromP = geometryFactory.createPoint(coordinateP);
        Point toP = geometryFactory.createPoint(coordinatetoP);*/
        Point fromP = new Point(envelope.getMinX(),envelope.getMinY());
        Point toP = new Point(envelope.getMaxX(),envelope.getMaxY());
        this.from = fromP;
        this.to = toP;
    }

    public boolean intersects(Rectangle r) {
        return this.to.getX() >= r.getFrom().getX() &&
                this.from.getX() <= r.getTo().getX() &&
                this.to.getY() >= r.getFrom().getY() &&
                this.from.getY() <= r.getTo().getY();
    }

    public boolean contains(Rectangle r) {
        return this.from.getX() <= r.getFrom().getX() &&
                this.from.getY() <= r.getFrom().getY() &&
                this.to.getX() >= r.getTo().getX() &&
                this.to.getY() >= r.getTo().getY();
    }

    public boolean contains(Point p) {
        return this.from.getX() <= p.getX() &&
                this.from.getY() <= p.getY() &&
                this.to.getX() >= p.getX() &&
                this.to.getY() >= p.getY();
    }
    public double getArea(){
        return (this.to.getX()-this.from.getX())*(this.to.getY()-this.from.getY());
    }
    public Point getFrom() {
        return from;
    }

    public Point getTo() {
        return to;
    }

    public void setFrom(Point from) {
        this.from = from;
    }

    public void setTo(Point to) {
        this.to = to;
    }

    public double getminX(){
        return this.getFrom().getX();
    }

    public double getmaxX(){
        return this.getTo().getX();
    }

    public double getminY(){
        return this.getFrom().getY();
    }

    public double getmaxY(){
        return this.getTo().getY();
    }



    @Override
    public String toString() {
        return "Rectangle{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rectangle rectangle = (Rectangle) o;
        return Objects.equals(from, rectangle.from) &&
                Objects.equals(to, rectangle.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    public Boolean contains(double x, double y) {
        if(x<=to.getX()&&x>=from.getX()&&y<=to.getY()&&y>=from.getY())return true;
        return false;
    }
}

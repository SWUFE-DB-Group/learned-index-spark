package datatypes;


public class Circle {
    private Point center;
    private double radius;

    public Circle(){}
    public Circle(Point center, double radius) {
        this.center = center;
        this.radius = radius;
    }

    public Rectangle getBoundingBox() {
        /*Coordinate coord1 = new Coordinate(center.getX() - radius, center.getY() - radius);
        Coordinate coord2 = new Coordinate(center.getX() + radius, center.getY() + radius);
        GeometryFactory geometryFactory = new GeometryFactory();
        Point point1 = geometryFactory.createPoint(coord1);
        Point point2 = geometryFactory.createPoint(coord2);*/
        Point point1 = new Point(center.getX() - radius, center.getY() - radius);
        Point point2 = new Point(center.getX() + radius, center.getY() + radius);
        Rectangle result = new Rectangle(point1,point2);
        return result;
    }

    public Point getCenter() {
        return center;
    }

    public void setCenter(Point center) {
        this.center = center;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    @Override
    public String toString() {
        return "Circle{" +
                "center=" + center +
                ", radius=" + radius +
                '}';
    }
}

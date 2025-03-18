package datatypes;



public class Edge {
    private Point from;
    private Point to;

    public Edge(Point from, Point to) {
        this.from = from;
        this.to = to;
    }

    public Point getFrom() {
        return from;
    }

    public void setFrom(Point from) {
        this.from = from;
    }

    public Point getTo() {
        return to;
    }

    public void setTo(Point to) {
        this.to = to;
    }
    @Override
    public String toString() {
        return "Edge from " + from + " to " + to;
    }


}

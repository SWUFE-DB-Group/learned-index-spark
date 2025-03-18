package spline;



import datatypes.Point;

import java.util.List;

public class CdfOnTheFlyInterfaceY {
    private int idx = 0;
    private Coord current;
    private List<Point> points;

    public CdfOnTheFlyInterfaceY(){}


    public CdfOnTheFlyInterfaceY(List<Point> points) {
        //assert !points.isEmpty();
        this.points = points;
        if (points.isEmpty()) {
            throw new IllegalArgumentException("The 'points' list must not be empty.");
        }
        double curr = points.get(idx).getY();
        while (idx + 1 < points.size() && curr == points.get(idx + 1).getY()) {
            idx++;
        }
        current = new Coord(points.get(idx).getY(), idx);
    }
    public Coord getCurrent() {
        return current;
    }
    public void next() {
        if (hasCurrent()) {
            idx++;
            if (idx == points.size()) {
                return;
            }

            double curr = points.get(idx).getY();
            while (idx + 1 < points.size() && curr == points.get(idx + 1).getY()) {
                idx++;
            }
            current = new Coord(points.get(idx).getY(), idx);
        }
    }
    public boolean hasCurrent() {
        return idx < points.size();
    }
    public int DataSize(){ return points.size();}

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public void setCurrent(Coord current) {
        this.current = current;
    }

    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }
}

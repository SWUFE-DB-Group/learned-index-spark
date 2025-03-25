package timeseries;

import datatypes.Point;

import java.util.Objects;

public class Timeseries<T> {
   private Point point;
   private T t;

    public Timeseries() {

    }
    public Timeseries(Point point, T t) {
        this.point = point;
        this.t = t;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    @Override
    public String toString() {
        return "Timeseries{" +
                "point=" + point +
                ", t=" + t +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Timeseries<?> that = (Timeseries<?>) o;
        return Objects.equals(point, that.point) &&
                Objects.equals(t, that.t);
    }

    @Override
    public int hashCode() {
        return Objects.hash(point, t);
    }

}

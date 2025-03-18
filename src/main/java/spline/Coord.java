package spline;

import java.io.Serializable;
import java.util.Objects;

//KeyType默认int
public class Coord implements Serializable {
    private double x;
    private double y;

    public Coord(){

    }

    public Coord(double x, double y) {
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

    @Override
    public String toString() {
        return "Coord{" +
                "x=" + x +
                ", y=" + y +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Coord coord = (Coord) o;
        return Double.compare(coord.x, x) == 0 &&
                Double.compare(coord.y, y) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
    /*public static void main(String[] args) {
        // 创建 Coord 对象
        Coord coord1 = new Coord(5.1, 3.5);
        Coord coord2 = new Coord(1.2, 42.0);

        // 访问 Coord 对象的属性
        System.out.println("coord1 x: " + coord1.getX() + ", y: " + coord1.getY());
        System.out.println("coord2 x: " + coord2.getX() + ", y: " + coord2.getY());
    }*/
}

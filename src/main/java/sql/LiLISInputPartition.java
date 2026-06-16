package sql;

import datatypes.Point;
import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

public class LiLISInputPartition implements InputPartition {
    private final List<Point> points;

    public LiLISInputPartition(List<Point> points) {
        this.points = points;
    }

    public List<Point> getPoints() {
        return points;
    }
}

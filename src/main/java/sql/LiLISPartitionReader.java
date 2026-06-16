package sql;

import datatypes.Point;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.io.IOException;
import java.util.Iterator;

public class LiLISPartitionReader implements PartitionReader<InternalRow> {
    private final Iterator<Point> iterator;
    private InternalRow current;

    public LiLISPartitionReader(LiLISInputPartition partition) {
        this.iterator = partition.getPoints().iterator();
    }

    @Override
    public boolean next() {
        if (!iterator.hasNext()) {
            return false;
        }
        Point point = iterator.next();
        current = new GenericInternalRow(new Object[]{point.getX(), point.getY()});
        return true;
    }

    @Override
    public InternalRow get() {
        return current;
    }

    @Override
    public void close() throws IOException {
    }
}

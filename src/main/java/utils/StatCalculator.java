package utils;

import datatypes.Point;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.Objects;

public class StatCalculator implements Serializable {
    private final Envelope boundary;
    private final long count;

    public StatCalculator(Envelope boundary, long count) {
        Objects.requireNonNull(boundary, "Boundary cannot be null");
        if (count <= 0L) {
            throw new IllegalArgumentException("Count must be > 0");
        } else {
            this.boundary = boundary;
            this.count = count;
        }
    }

    public static StatCalculator combine(StatCalculator agg1, StatCalculator agg2) throws Exception {
        if (agg1 == null) {
            return agg2;
        } else {
            return agg2 == null ? agg1 : new StatCalculator(combine(agg1.boundary, agg2.boundary), agg1.count + agg2.count);
        }
    }

    public static Envelope combine(Envelope agg1, Envelope agg2) throws Exception {
        if (agg1 == null) {
            return agg2;
        } else {
            return agg2 == null ? agg1 : new Envelope(Math.min(agg1.getMinX(), agg2.getMinX()), Math.max(agg1.getMaxX(), agg2.getMaxX()), Math.min(agg1.getMinY(), agg2.getMinY()), Math.max(agg1.getMaxY(), agg2.getMaxY()));
        }
    }

    public static Envelope add(Envelope agg, Point object) throws Exception {

        return combine(object.getEnvelopeInternal(), agg);
    }

    public static StatCalculator add(StatCalculator agg, Point object) throws Exception {
        return combine(new StatCalculator(object.getEnvelopeInternal(), 1L), agg);
    }

    public Envelope getBoundary() {
        return this.boundary;
    }

    public long getCount() {
        return this.count;
    }
}

package sql;

import datatypes.Rectangle;

import java.io.Serializable;

class LiLISRange implements Serializable {
    private Double xMin;
    private Double yMin;
    private Double xMax;
    private Double yMax;

    void updateLower(String column, double value) {
        if ("x".equalsIgnoreCase(column)) {
            xMin = xMin == null ? value : Math.max(xMin, value);
        } else if ("y".equalsIgnoreCase(column)) {
            yMin = yMin == null ? value : Math.max(yMin, value);
        }
    }

    void updateUpper(String column, double value) {
        if ("x".equalsIgnoreCase(column)) {
            xMax = xMax == null ? value : Math.min(xMax, value);
        } else if ("y".equalsIgnoreCase(column)) {
            yMax = yMax == null ? value : Math.min(yMax, value);
        }
    }

    boolean isComplete() {
        return xMin != null && yMin != null && xMax != null && yMax != null;
    }

    Rectangle toRectangle() {
        if (!isComplete()) {
            throw new IllegalStateException("Range is incomplete");
        }
        return new Rectangle(Math.min(xMin, xMax), Math.min(yMin, yMax), Math.max(xMin, xMax), Math.max(yMin, yMax));
    }

    boolean contains(double x, double y) {
        return (xMin == null || x >= xMin)
                && (xMax == null || x <= xMax)
                && (yMin == null || y >= yMin)
                && (yMax == null || y <= yMax);
    }
}

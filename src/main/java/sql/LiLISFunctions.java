package sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;

public class LiLISFunctions {
    private LiLISFunctions() {
    }

    public static void registerAll(SparkSession spark) {
        spark.udf().register("lilis_in_range", new UDF6<Double, Double, Double, Double, Double, Double, Boolean>() {
            @Override
            public Boolean call(Double x, Double y, Double xMin, Double yMin, Double xMax, Double yMax) {
                if (hasNull(x, y, xMin, yMin, xMax, yMax)) {
                    return false;
                }
                return x >= xMin && x <= xMax && y >= yMin && y <= yMax;
            }
        }, DataTypes.BooleanType);

        spark.udf().register("lilis_in_distance", new UDF5<Double, Double, Double, Double, Double, Boolean>() {
            @Override
            public Boolean call(Double x, Double y, Double queryX, Double queryY, Double distance) {
                if (hasNull(x, y, queryX, queryY, distance)) {
                    return false;
                }
                double dx = x - queryX;
                double dy = y - queryY;
                return Math.sqrt(dx * dx + dy * dy) <= distance;
            }
        }, DataTypes.BooleanType);

        spark.udf().register("lilis_distance", new UDF4<Double, Double, Double, Double, Double>() {
            @Override
            public Double call(Double x, Double y, Double queryX, Double queryY) {
                if (hasNull(x, y, queryX, queryY)) {
                    return null;
                }
                double dx = x - queryX;
                double dy = y - queryY;
                return Math.sqrt(dx * dx + dy * dy);
            }
        }, DataTypes.DoubleType);

        spark.udf().register("lilis_is_point", new UDF4<Double, Double, Double, Double, Boolean>() {
            @Override
            public Boolean call(Double x, Double y, Double queryX, Double queryY) {
                if (hasNull(x, y, queryX, queryY)) {
                    return false;
                }
                return x.doubleValue() == queryX.doubleValue() && y.doubleValue() == queryY.doubleValue();
            }
        }, DataTypes.BooleanType);
    }

    private static boolean hasNull(Object... values) {
        for (Object value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }
}

package utils;

import datatypes.Point;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * 从 HDFS 读取 Point 数据的工具类
 *
 * 用法示例:
 *   // 默认读取: x=第0列, y=第1列, 有表头
 *   JavaRDD<Point> points = HDFSPointReader.readCSV(sc, "hdfs://namenode:9000/data/points.csv");
 *
 *   // 指定列: x=第5列, y=第6列, 有表头
 *   JavaRDD<Point> points = HDFSPointReader.readCSV(sc, "hdfs:///data/taxi.csv", 5, 6, true);
 *
 *   // 无表头
 *   JavaRDD<Point> points = HDFSPointReader.readCSV(sc, "hdfs:///data/points.csv", 0, 1, false);
 *
 *   // 自定义分隔符 (如 TSV)
 *   JavaRDD<Point> points = HDFSPointReader.readCSV(sc, "hdfs:///data/points.tsv", 0, 1, true, "\t");
 */
public class HDFSPointReader implements Serializable {

    private static final String DEFAULT_DELIMITER = ",";

    /**
     * 从 HDFS 读取 CSV 格式的 Point 数据
     *
     * @param sc        JavaSparkContext
     * @param path      HDFS 文件路径 (如 hdfs://namenode:9000/data/points.csv)
     * @param xOffset   x 坐标所在列索引 (从 0 开始)
     * @param yOffset   y 坐标所在列索引 (从 0 开始)
     * @param hasHeader 是否包含表头行
     * @param delimiter 列分隔符
     * @return JavaRDD<Point>
     */
    public static JavaRDD<Point> readCSV(JavaSparkContext sc, String path,
                                         int xOffset, int yOffset,
                                         boolean hasHeader, String delimiter) {
        JavaRDD<String> rawRDD = sc.textFile(path);

        if (hasHeader) {
            String header = rawRDD.first();
            rawRDD = rawRDD.filter(line -> !line.equals(header));
        }

        final String delim = delimiter;
        final int xCol = xOffset;
        final int yCol = yOffset;

        return rawRDD.map(line -> {
            String[] fields = line.split(delim);
            double x = Double.parseDouble(fields[xCol].trim());
            double y = Double.parseDouble(fields[yCol].trim());
            return new Point(x, y);
        });
    }

    /**
     * 从 HDFS 读取 CSV 格式的 Point 数据（逗号分隔）
     *
     * @param sc        JavaSparkContext
     * @param path      HDFS 文件路径
     * @param xOffset   x 坐标所在列索引 (从 0 开始)
     * @param yOffset   y 坐标所在列索引 (从 0 开始)
     * @param hasHeader 是否包含表头行
     * @return JavaRDD<Point>
     */
    public static JavaRDD<Point> readCSV(JavaSparkContext sc, String path,
                                         int xOffset, int yOffset, boolean hasHeader) {
        return readCSV(sc, path, xOffset, yOffset, hasHeader, DEFAULT_DELIMITER);
    }

    /**
     * 从 HDFS 读取 CSV 格式的 Point 数据
     * 默认: x=第0列, y=第1列, 有表头, 逗号分隔
     *
     * @param sc   JavaSparkContext
     * @param path HDFS 文件路径
     * @return JavaRDD<Point>
     */
    public static JavaRDD<Point> readCSV(JavaSparkContext sc, String path) {
        return readCSV(sc, path, 0, 1, true, DEFAULT_DELIMITER);
    }

    /**
     * 从 HDFS 读取 CSV 格式的 Point 数据（无表头）
     * 默认: x=第0列, y=第1列, 逗号分隔
     *
     * @param sc   JavaSparkContext
     * @param path HDFS 文件路径
     * @return JavaRDD<Point>
     */
    public static JavaRDD<Point> readCSVNoHeader(JavaSparkContext sc, String path) {
        return readCSV(sc, path, 0, 1, false, DEFAULT_DELIMITER);
    }
}

package utils;


import datatypes.Point;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadPoints {

    public static void readPointsToJson(String filepath, List<Point>points){
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            StringBuilder jsonText = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                jsonText.append(line);
                String json = jsonText.toString();
                Point point = parseCoordinatesFromJson(json);

                points.add(point);

                jsonText = new StringBuilder();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Point parseCoordinatesFromJson(String json) {
        Pattern pattern = Pattern.compile("\"g\":\"POINT \\((-?\\d+\\.\\d+) (-?\\d+\\.\\d+)\\)\"");
        Matcher matcher = pattern.matcher(json);

        if (matcher.find()) {
            double longitude = Double.parseDouble(matcher.group(1));
            double latitude = Double.parseDouble(matcher.group(2));

            /*Coordinate coordinate = new Coordinate(longitude, latitude);
            GeometryFactory geometryFactory = new GeometryFactory();
            Point point = geometryFactory.createPoint(coordinate);*/
            Point point = new Point(longitude,latitude);
            return point;
        }

        return null;
    }
    public static List<Point> readPointsFromCSV(String fileName) {
        List<Point> points = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            // 跳过CSV头部
            String line = reader.readLine();

            // 读取点数据
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                double x = Double.parseDouble(data[0]);
                double y = Double.parseDouble(data[1]);
                points.add(new Point(x, y));
            }

            System.out.println("从CSV文件中读取点成功：" + fileName);
        } catch (IOException e) {
            System.err.println("读取CSV文件时发生错误：" + e.getMessage());
            e.printStackTrace();
        }

        return points;
    }


}

package utils;


import datatypes.Rectangle;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadPolygon {
    /*private static List<Polygon> readPolygonsFromCSV(String filename) throws IOException {

        List<Coordinate> coordinates = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();
        int i=0;

        try (CSVParser parser = new CSVParser(new FileReader(filename), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                double fromX = Double.parseDouble(record.get("fromX"));
                double fromY = Double.parseDouble(record.get("fromY"));
                double toX = Double.parseDouble(record.get("toX"));
                double toY = Double.parseDouble(record.get("toY"));
                Coordinate coordinate1 = new Coordinate(fromX, fromY);
                Coordinate coordinate2 = new Coordinate(fromX, fromY);
                coordinates.add(coordinate1);
                coordinates.add(coordinate2);
                if(coordinates.size()==4){
                    coordinates.add(coordinates.get(0));
                    GeometryFactory geometryFactory = new GeometryFactory();
                    Coordinate[] coordArray = coordinates.toArray(new Coordinate[0]);
                    LinearRing linearRing = geometryFactory.createLinearRing(coordArray);
                    Polygon polygon = geometryFactory.createPolygon(linearRing);
                    polygons.add(polygon);
                    coordinates.clear();
                    i++;
                }
                if(i>100)break;
            }
        }

        return polygons;
    }*/
    /*public static List<Polygon> readPolygonsFromCSV(String filename) throws IOException {
        List<Coordinate> coordinates = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();

        CSVFormat csvFormat = CSVFormat.DEFAULT
                .withHeader("fromX", "fromY", "toX", "toY")
                .withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new FileReader(filename), csvFormat)) {
            for (CSVRecord record : parser) {
                double fromX = Double.parseDouble(record.get("fromX"));
                double fromY = Double.parseDouble(record.get("fromY"));
                double toX = Double.parseDouble(record.get("toX"));
                double toY = Double.parseDouble(record.get("toY"));
                Coordinate coordinate1 = new Coordinate(fromX, fromY);
                Coordinate coordinate2 = new Coordinate(fromX, toY);
                Coordinate coordinate3 = new Coordinate(toX, fromY);
                Coordinate coordinate4 = new Coordinate(toX, toY);
                coordinates.add(coordinate1);
                coordinates.add(coordinate2);
                coordinates.add(coordinate3);
                coordinates.add(coordinate4);
                if(coordinates.size()==4){
                    coordinates.add(coordinates.get(0));
                    GeometryFactory geometryFactory = new GeometryFactory();
                    Coordinate[] coordArray = coordinates.toArray(new Coordinate[0]);
                    LinearRing linearRing = geometryFactory.createLinearRing(coordArray);
                    Polygon polygon = geometryFactory.createPolygon(linearRing);
                    polygons.add(polygon);
                    coordinates.clear();

                }

            }
        }
        return polygons;
    }*/
    public static List<Polygon> readPolygonsFromCSV(String fileName) {
        Map<Integer, List<Coordinate>> polygonCoordinatesMap = new HashMap<>();
        GeometryFactory geometryFactory = new GeometryFactory();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            br.readLine(); // 跳过表头行

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                int polygonId = Integer.parseInt(values[0]);
                double coordinateX = Double.parseDouble(values[1]);
                double coordinateY = Double.parseDouble(values[2]);

                polygonCoordinatesMap.putIfAbsent(polygonId, new ArrayList<>());
                polygonCoordinatesMap.get(polygonId).add(new Coordinate(coordinateX, coordinateY));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Polygon> polygons = new ArrayList<>();
        for (Map.Entry<Integer, List<Coordinate>> entry : polygonCoordinatesMap.entrySet()) {
            List<Coordinate> coordinates = entry.getValue();
            // 确保多边形闭合
            if (!coordinates.get(0).equals(coordinates.get(coordinates.size() - 1))) {
                coordinates.add(coordinates.get(0));
            }
            Coordinate[] coordsArray = coordinates.toArray(new Coordinate[0]);
            LinearRing linearRing = geometryFactory.createLinearRing(coordsArray);
            Polygon polygon = geometryFactory.createPolygon(linearRing);
            polygons.add(polygon);
        }

        return polygons;
    }

}

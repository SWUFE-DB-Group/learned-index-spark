package utils;

import datatypes.Rectangle;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadRectangles {
    public static List<Rectangle> readRectanglesFromCSV(String filename) throws IOException {
        List<Rectangle> rectangles = new ArrayList<>();
        CSVFormat csvFormat = CSVFormat.DEFAULT
                .withHeader("fromX", "fromY", "toX", "toY")
                .withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new FileReader(filename), csvFormat)) {
            for (CSVRecord record : parser) {
                double fromX = Double.parseDouble(record.get("fromX"));
                double fromY = Double.parseDouble(record.get("fromY"));
                double toX = Double.parseDouble(record.get("toX"));
                double toY = Double.parseDouble(record.get("toY"));
                rectangles.add(new Rectangle(fromX, fromY, toX, toY));
            }
        }
        return rectangles;
    }
}

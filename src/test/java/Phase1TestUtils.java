import datatypes.Point;
import datatypes.Rectangle;
import spline.Spline;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Phase1TestUtils {
    private Phase1TestUtils() {
    }

    public static List<Point> readPointsFromCsv(String relativePath) throws IOException {
        Path path = Paths.get(relativePath);
        if (!Files.exists(path)) {
            throw new IOException("Dataset not found: " + relativePath + ". Generate it using data/make_phase1_datasets.py");
        }

        List<Point> points = new ArrayList<Point>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                String[] parts = trimmed.split(",");
                if (parts.length < 2) {
                    continue;
                }
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                points.add(new Point(x, y));
            }
        }

        return points;
    }

    public static Spline buildSpline(List<Point> sourcePoints) {
        Spline spline = new Spline();
        spline.setPoints(new ArrayList<Point>(sourcePoints));
        spline.build();
        return spline;
    }

    public static boolean bruteForceContains(List<Point> points, Point query) {
        for (Point point : points) {
            if (point.equals(query)) {
                return true;
            }
        }
        return false;
    }

    public static List<Point> bruteForceRange(List<Point> points, Rectangle rectangle) {
        List<Point> result = new ArrayList<Point>();
        for (Point point : points) {
            if (rectangle.contains(point)) {
                result.add(point);
            }
        }
        return result;
    }

    public static List<Point> bruteForceTopK(List<Point> points, Point query, int k) {
        List<Point> copy = new ArrayList<Point>(points);
        Collections.sort(copy, new Comparator<Point>() {
            @Override
            public int compare(Point a, Point b) {
                int byDist = Double.compare(a.distanceTo(query), b.distanceTo(query));
                if (byDist != 0) {
                    return byDist;
                }
                int byX = Double.compare(a.getX(), b.getX());
                if (byX != 0) {
                    return byX;
                }
                return Double.compare(a.getY(), b.getY());
            }
        });
        return new ArrayList<Point>(copy.subList(0, k));
    }

    public static Map<String, Integer> toMultiset(List<Point> points) {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (Point point : points) {
            String key = point.getX() + "," + point.getY();
            Integer current = counts.get(key);
            counts.put(key, current == null ? 1 : current + 1);
        }
        return counts;
    }
}

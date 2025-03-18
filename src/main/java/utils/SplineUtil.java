package utils;


import org.locationtech.jts.geom.Point;
import spline.CdfOnTheFlyInterfaceY;
import spline.Coord;
import spline.Errors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SplineUtil {
    public static double interpolate(List<Coord> spline, double pos) {
        Coord prev = null;
        Coord next = null;

        for (Coord coord : spline) {
            if (coord.getX() < pos) {
                prev = coord;
            } else if (coord.getX() >= pos) {
                next = coord;
                break;
            }
        }

        if (prev == null) {
            return next.getY();
        } else if (next == null) {
            return prev.getY();
        } else {
            double dx = next.getX() - prev.getY();
            double dy = next.getY() - prev.getY();
            double ofs = pos - prev.getY();
            return prev.getY() + ofs * (dy / dx);
        }
    }

    public static Errors computeErrors(List<Coord> cdf, List<Coord> spline) {
        Errors errs = new Errors();

        for (Coord elem : cdf) {
            double pos = elem.getX();
            double estimate = interpolate(spline, pos);
            double real = elem.getY();
            double error = estimate - real;

            if (error < 0) {
                error = -error;
            }

            if (error > errs.getMaximum()) {
                errs.maximum = error;
            }

            errs.average += error;
        }

        errs.average /= cdf.size();
        return errs;
    }
    public static void printErrors(List<Coord> cdf, List<Coord> spline) {
        Errors errs = computeErrors(cdf, spline);
        System.out.println("cdf_size: " + cdf.size());
        System.out.println("spline_size: " + spline.size());
        System.out.println("Errors for spline - (average, max): (" + errs.average + ", " + errs.maximum + ")");
    }

    public static int cmpDevs(Coord a, Coord b, Coord c) {
        double dx1 = b.getX() - a.getX();
        double dx2 = c.getX() - a.getX();
        double dy1 = b.getY() - a.getY();
        double dy2 = c.getY() - a.getY();

        if ((dy1 * dx2) < (dy2 * dx1)) {
            return -1;
        } else if ((dy1 * dx2) > (dy2 * dx1)) {
            return 1;
        }
        return 0;
    }

    public static List<Coord> tautString(List<Coord> data, double maxValue, double epsilon) {
        List<Coord> result = new ArrayList<>();

        if (data.isEmpty()) {
            return result;
        }

        result.add(data.get(0));

        if (data.size() == 1) {
            return result;
        }

        Iterator<Coord> iter = data.iterator();
        Coord upperLimit = new Coord(0, 0);
        Coord lowerLimit = new Coord(0, 0);
        Coord last = result.get(result.size() - 1);

        while (iter.hasNext()) {
            Coord current = iter.next();
            Coord u = new Coord(current.getX(), current.getY() + epsilon);
            Coord l = new Coord(current.getX(), current.getY() - epsilon);
            Coord b = last;

            // Check if we cut the error corridor
            if ((!last.equals(b)) && (cmpDevs(b, upperLimit, current) < 0 || cmpDevs(b, lowerLimit, current) > 0)) {
                result.add(last);
                b = last;
            }

            // Update the error margins
            if (last.equals(b) || cmpDevs(b, upperLimit, u) > 0) {
                upperLimit = u;
            }
            if (last.equals(b) || cmpDevs(b, lowerLimit, l) < 0) {
                lowerLimit = l;
            }

            // Remember the current point
            last = current;
        }

        result.add(data.get(data.size() - 1));

        return result;
    }

    public static List<Coord> compressFunc(List<Coord> func, int desiredSize) {
        // Relax a bit to speed up compression
        int maxSize = desiredSize + (desiredSize / 100);
        int minSize = desiredSize - (desiredSize / 100);

        // Fits?
        if (func.size() <= maxSize) {
            return func;
        }

        // No, binary search for a good enough epsilon (== middle) in (0, func.size())
        int left = 0;
        int right = func.size();

        while (left < right) {
            int middle = (left + right) / 2;
            List<Coord> candidate = tautString(func, func.get(func.size() - 1).getY(), middle);

            if (candidate.size() < minSize) {
                right = middle;
            } else if (candidate.size() > maxSize) {
                left = middle + 1;
            } else {
                return candidate;
            }
        }

        // Final call, this is the best we could get
        return tautString(func, func.get(func.size() - 1).getY(), left);
    }

    public static List<Double> computeSlopes(List<Coord> spline) {
        int splineSize = spline.size();
        List<Double> slopes = new ArrayList<>(splineSize - 1);

        for (int index = 0; index < splineSize - 1; ++index) {
            double dx = spline.get(index).getX() - spline.get(index + 1).getX();
            double dy = spline.get(index).getY() - spline.get(index + 1).getY();
            slopes.add(dy / dx);
        }

        return slopes;
    }
    public static List<Coord> buildCdf(List<Point> points) {
        List<Coord> cdf = new ArrayList<>();

        int pos = 0;
        double last = points.get(0).getY();

        for (int i = 0; i < points.size(); i++) {
            Point d = points.get(i);
            if (d.getY() != last) {
                cdf.add(new Coord(last, pos - 1));
                last = d.getY();
            }
            pos++;
        }

        cdf.add(new Coord(last, pos - 1));

        return cdf;
    }



    public static List<Coord> tautString(CdfOnTheFlyInterfaceY cdf, double epsilon) {
        List<Coord> result = new ArrayList<>();
        //result.ensureCapacity(cdf.DataSize() * 2 / epsilon);

        if (!cdf.hasCurrent())
        return result;

        result.add(cdf.getCurrent());
        cdf.next();

        if (!cdf.hasCurrent())
        return result;

        Coord upperLimit = null;
        Coord lowerLimit = null;
        Coord last = result.get(result.size() - 1);

        while (cdf.hasCurrent()) {
            Coord u = cdf.getCurrent();
            Coord l = cdf.getCurrent();
            Coord b = result.get(result.size() - 1);
            u.setY(u.getY()+epsilon); //u.second += epsilon;
            l.setY(l.getY()-epsilon);//l.second -= epsilon;

            if (!last.equals(b) && (cmpDevs(b, upperLimit, cdf.getCurrent()) < 0
                    || cmpDevs(b, lowerLimit, cdf.getCurrent()) > 0)) {
                result.add(last);
                b = last;
            }

            if (last.equals(b) || cmpDevs(b, upperLimit, u) > 0)
                upperLimit = u;

            if (last.equals(b) || cmpDevs(b, lowerLimit, l) < 0)
                lowerLimit = l;

            last = cdf.getCurrent();
            cdf.next();
        }

        result.add(cdf.getCurrent());

        return result;
    }

}

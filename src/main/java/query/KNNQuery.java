package query;

import datatypes.Point;
import datatypes.Rectangle;
import org.apache.spark.api.java.JavaRDD;
import spline.Spline;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


public class KNNQuery {
    KNNQuery (){}
    //暂时使用double maxArea,double N的参数，整合后在SpatialRDD中获取
    public static List<Point> SpatialKNNQuery(JavaRDD<Spline> pointRDD, int k, Point querypiont, double maxArea, double N) {
        double area = maxArea;
        double r;
        double dmbb;
        dmbb = N/area;
        double x = querypiont.getX();
        double y = querypiont.getY();
        r = Math.sqrt((k/(Math.PI*dmbb)));
        List<Point>res = new ArrayList<>();
        while(res.size()<k){
            Point star = new Point(x-r,y-r);
            Point end = new Point(x+r,y+r);
            Rectangle rec = new Rectangle(star,end);

            res = RangeQuery.SpatialRangeQuery(pointRDD, rec).collect();


            if(res.size()>=k)break;
            if(res.size()==0){
                r = 2*(rec.getTo().getX()-rec.getFrom().getX());
            }
            else{
                area = rec.getArea();
                dmbb = res.size()/area;
                r = Math.sqrt((k/(Math.PI*dmbb)));
            }
        }
        /*Collections.sort(res, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                double distance1 = querypiont.euclideanDistance(o1);
                double distance2 = querypiont.euclideanDistance(o2);
                return Double.compare(distance1, distance2);
            }
        });*/
        List<Point> topKPoints = pointSort(res,querypiont,k);
        return topKPoints;
    }
    public static List<Point> SpatialKNNQuerywithoutIndex(JavaRDD<Point> pointRDD, int k, Point querypiont, double maxArea, double N) {
        double area = maxArea;
        double r;
        double dmbb;
        dmbb = N/area;
        double x = querypiont.getX();
        double y = querypiont.getY();
        r = Math.sqrt((k/(Math.PI*dmbb)));
        List<Point>res = new ArrayList<>();
        while(res.size()<k){
            Point star = new Point(x-r,y-r);
            Point end = new Point(x+r,y+r);
            Rectangle rec = new Rectangle(star,end);

            res = RangeQuery.SpatialRangeQueryWithOutIndex(pointRDD, rec).collect();


            if(res.size()>=k)break;
            if(res.size()==0){
                r = 2*(rec.getTo().getX()-rec.getFrom().getX());
            }
            else{
                area = rec.getArea();
                dmbb = res.size()/area;
                r = Math.sqrt((k/(Math.PI*dmbb)));
            }
        }
        /*Collections.sort(res, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                double distance1 = querypiont.euclideanDistance(o1);
                double distance2 = querypiont.euclideanDistance(o2);
                return Double.compare(distance1, distance2);
            }
        });*/
        List<Point> topKPoints = pointSort(res,querypiont,k);
        return topKPoints;
    }
    public static   List<Point> pointSort(List<Point> points, Point center, int K){

        List<Point>res = new ArrayList<>(K);
        PriorityQueue<Point> maxHeap = new PriorityQueue<>(K, new Comparator<Point>() {
            public int compare(Point p1, Point p2) {
                double dist1 = p1.distanceTo(center);
                double dist2 = p2.distanceTo(center);
                return Double.compare(dist2, dist1);
            }
        });
         for (Point point : points) {
            if (maxHeap.size() < K) {
                maxHeap.offer(point);
            } else {
                if (point.distanceTo(center) < maxHeap.peek().distanceTo(center)) {
                    maxHeap.poll();
                    maxHeap.offer(point);
                }
            }
        }

        for (int i = 0; i < K; i++) {
            res.add(maxHeap.poll());
        }


        return res;
    }




}

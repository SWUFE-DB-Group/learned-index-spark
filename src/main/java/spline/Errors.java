package spline;

public class Errors {
    public double average;
    public double maximum;

    public Errors() {
        this.average = 0.0;
        this.maximum = 0.0;
    }

    public double getAverage() {
        return average;
    }

    public double getMaximum() {
        return maximum;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public void setMaximum(double maximum) {
        this.maximum = maximum;
    }
}

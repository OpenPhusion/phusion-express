package cloud.phusion.express.util;

public class TimeMarker {
    private long t;

    public TimeMarker() {
        this.t = System.nanoTime();
    }

    public double mark() {
        long t1 = this.t;
        long t2 = System.nanoTime();
        this.t = t2;
        return (t2-t1)/100000/10.0;
    }

    public void markAndPrint() {
        markAndPrint("Run");
    }

    public void markAndPrint(String action) {
        long t1 = this.t;
        long t2 = System.nanoTime();
        this.t = t2;

        System.out.println(String.format("%s in %.1fms", action, (t2-t1)/100000/10.0));
    }

}

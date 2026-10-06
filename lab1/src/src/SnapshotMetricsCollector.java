import static java.lang.Math.min;

public class SnapshotMetricsCollector implements MetricsCollector {
    long[] buckets = new long[256];
    long count = 0;
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = 0;

    @Override
    public void record(long value) {
        buckets[(int) min(value / 4, 255)]++;
        count++;
        sum += value;
        if (value < min) min = value;
        if (value > max) max = value;
    }

    @Override
    public Snapshot snapshot() {
        long[] copy = buckets.clone();

        return new Snapshot(copy, count, sum, min, max,
                percentile(copy, count, .5),
                percentile(copy, count, .99));
    }

    private static long percentile(long[] buckets, long count, double q) {
        double threshold = count * q;
        long acc = 0;

        for (int i = 0; i < buckets.length; i++) {
            acc += buckets[i];

            if (acc >= threshold)
                return i * 4L;
        }

        return (buckets.length - 1) * 4L;
    }
}

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.Math.min;

public class SnapshotMetricsCollector implements MetricsCollector {
    long[] buckets = new long[256];
    Object[] locks = Stream.generate(Object::new)
            .limit(16)
            .toArray();

    AtomicLong count = new AtomicLong(0);
    AtomicLong sum = new AtomicLong(0);
    AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    AtomicLong max = new AtomicLong(0);

    @Override
    public void record(long value) {
        int b = Math.toIntExact(min(value / 4, 255));

        synchronized (locks[b % 16]) {
            buckets[b]++;
        }

        count.incrementAndGet();
        sum.getAndAdd(value);

        long minimum = min.get();
        while (value < minimum) {
            if (min.compareAndSet(minimum, value)) break;
            minimum = min.get();
        }

        long maximum = max.get();
        while (value > maximum) {
            if (max.compareAndSet(maximum, value)) break;
            maximum = max.get();
        }
    }

    @Override
    public synchronized Snapshot snapshot() {
        long[] copy = buckets.clone();

        long total = Arrays.stream(copy).sum();

        return new Snapshot(copy, count.get(), sum.get(), min.get(), max.get(),
                percentile(copy, total, .5),
                percentile(copy, total, .99));
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

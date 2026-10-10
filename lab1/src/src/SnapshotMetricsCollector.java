import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SnapshotMetricsCollector implements MetricsCollector {
    private volatile int active = 0;

    private final Object snapLock = new Object();
    private final List<ThreadBuffers> allBuffers = new ArrayList<>();

    private final long[] globalBuckets = new long[256];
    private long globalCount = 0;
    private long globalSum = 0;
    private long globalMin = Long.MAX_VALUE;
    private long globalMax = 0;

    private final ThreadLocal<ThreadBuffers> myBuffers = ThreadLocal.withInitial(() -> {
        ThreadBuffers buffers = new ThreadBuffers();
        synchronized (snapLock) {
            allBuffers.add(buffers);
        }
        return buffers;
    });

//    private final List<ThreadState> allStates = new ArrayList<>();
//    private final Object listLock = new Object();
//
//    private final ThreadLocal<ThreadState> myState = ThreadLocal.withInitial(() -> {
//        ThreadState s = new ThreadState();
//        synchronized (listLock) {
//            allStates.add(s);
//        }
//        return s;
//    });

//    long[] buckets = new long[256];
//    Object[] locks = Stream.generate(Object::new)
//            .limit(16)
//            .toArray();
//
//    AtomicLong count = new AtomicLong(0);
//    AtomicLong sum = new AtomicLong(0);
//    AtomicLong min = new AtomicLong(Long.MAX_VALUE);
//    AtomicLong max = new AtomicLong(0);

    @Override
    public void record(long value) {
        ThreadBuffers my = myBuffers.get();
        int b;

        while (true) {
            b = active;
            my.inside.set(b);
            if (active == b) break;
            my.inside.setRelease(-1);
        }

        int bucket = (int) Math.min(value / 4, 255);
        my.buckets[b][bucket]++;
        my.count[b]++;
        my.sum[b] += value;
        if (value < my.min[b]) my.min[b] = value;
        if (value > my.max[b]) my.max[b] = value;

        my.inside.setRelease(-1);
    }

    @Override
    public Snapshot snapshot() {
        synchronized (snapLock) {
            int old = active;
            active = 1 - old;

            for (ThreadBuffers s : allBuffers) {
                while (s.inside.get() == old)
                    Thread.onSpinWait();

                for (int i = 0; i < 256; i++) globalBuckets[i] += s.buckets[old][i];
                globalCount += s.count[old];
                globalSum += s.sum[old];
                globalMin = Math.min(globalMin, s.min[old]);
                globalMax = Math.max(globalMax, s.max[old]);

                Arrays.fill(s.buckets[old], 0);
                s.count[old] = 0;
                s.sum[old] = 0;
                s.min[old] = Long.MAX_VALUE;
                s.max[old] = 0;
            }

            long[] out = globalBuckets.clone();
            return new Snapshot(out, globalCount, globalSum, globalMin, globalMax,
                    computePercentile(out, globalCount, 0.50),
                    computePercentile(out, globalCount, 0.99));
        }
    }

    private static long computePercentile(long[] buckets, long count, double q) {
        double threshold = count * q;
        long acc = 0;

        for (int i = 0; i < buckets.length; i++) {
            acc += buckets[i];

            if (acc >= threshold) return i * 4L;
        }

        return (buckets.length - 1) * 4L;
    }
}

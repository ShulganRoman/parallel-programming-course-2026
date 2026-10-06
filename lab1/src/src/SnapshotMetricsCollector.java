import java.util.ArrayList;
import java.util.List;

public class SnapshotMetricsCollector implements MetricsCollector {
    private final List<ThreadState> allStates = new ArrayList<>();
    private final Object listLock = new Object();

    private final ThreadLocal<ThreadState> myState = ThreadLocal.withInitial(() -> {
        ThreadState s = new ThreadState();
        synchronized (listLock) {
            allStates.add(s);
        }
        return s;
    });

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
        ThreadState s = myState.get();
        int b = (int) Math.min(value / 4, 255);

        s.buckets.setRelease(b, s.buckets.getPlain(b) + 1);
        s.count.setRelease(s.count.getPlain() + 1);
        s.sum.setRelease(s.sum.getPlain() + value);

        // min и max обновляются обычным сравнением без CAS:
        if (value < s.min.getPlain()) s.min.setRelease(value);
        if (value > s.max.getPlain()) s.max.setRelease(value);
    }

    @Override
    public synchronized Snapshot snapshot() {
        List<ThreadState> copyOfStates = List.copyOf(allStates);

        long[] out = new long[256];
        long count = 0, sum = 0, min = Long.MAX_VALUE, max = 0;

        for (ThreadState s : copyOfStates) {
            for (int i = 0; i < 256; i++) out[i] += s.buckets.get(i); // C++: .load(relaxed)
            count += s.count.get();

            sum += s.sum.get();
            min = Math.min(min, s.min.get());
            max = Math.max(max, s.max.get());
        }
// считаем p50 и p99 кумулятивно по массиву out (см. Этап 0)
        long p50 = computePercentile(out, count, 0.50);
        long p99 = computePercentile(out, count, 0.99);
        return new Snapshot(out, count, sum, min, max, p50, p99);
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

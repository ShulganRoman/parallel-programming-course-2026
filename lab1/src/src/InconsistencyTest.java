import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class InconsistencyTest {
    public static void run(MetricsCollector collector, int[] values) throws InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(4);
        Thread[] writers = new Thread[4];

        for (int k = 0; k < 4; k++) {
            int finalK = k;

            writers[k] = new Thread(() -> {
                int i = finalK * values.length / 4;

                started.countDown();

                while (!stop.get()) {
                    collector.record(values[i]);
                    i++;
                    if (i == values.length) i = 0;
                }
            });

            writers[k].start();
        }

        started.await();

        int less = 0;
        int greater = 0;

        for (int s = 0; s < 10000; s++) {
            Snapshot snapshot = collector.snapshot();
            long bucketsSum = Arrays.stream(snapshot.buckets()).sum();

            if (bucketsSum < snapshot.count()) less++;
            else if (bucketsSum > snapshot.count()) greater++;
        }

        stop.set(true);

        for (Thread writer : writers)
            writer.join();

        int broken = less + greater;
        System.out.printf("битые: %d из %d (%.2f%%)%n", broken, 10000, 100.0 * broken / 10000);
        System.out.println("меньше = " + less);
        System.out.println("больше = " + greater);
    }
}

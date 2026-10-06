void main() throws InterruptedException {
    var latencies = generateLatencies();

    InconsistencyTest.run(new SnapshotMetricsCollector(), latencies);

    List<Integer> list = List.of(1, 2, 4, 8, 16);
    for (var threads : list)
        System.out.println("median in " + threads + " = " + measurePoint(new SnapshotMetricsCollector(), latencies, threads));
}

private static int[] generateLatencies() {
    double sum = 0;
    double[] array = new double[1024];

    for (int i = 1; i < 1024; i++)
        sum += 1 / Math.pow(i, 1.15);

    double acc = 0;

    for (int i = 1; i < 1024; i++) {
        acc += 1 / Math.pow(i, 1.15);
        array[i] = acc / sum;
    }

    Random rnd = new Random(42);

    int[] latencies = new int[(int) Math.pow(2, 20)];

    for (int i = 0; i < latencies.length; i++) {
        int r = 1024;
        int l = 0;
        int mid = 0;

        double random = rnd.nextDouble();

        while (r > l) {
            mid = (r + l) / 2;

            if (random >= array[mid]) l = mid + 1;
            else r = mid;
        }

        latencies[i] = mid;
    }

    return latencies;
}

public static int run(MetricsCollector collector, int[] values, int T, long millis) throws InterruptedException {
    AtomicBoolean stop = new AtomicBoolean(false);
    CountDownLatch start = new CountDownLatch(T + 1);
    long[] ops = new long[T];

    ExecutorService executor = Executors.newFixedThreadPool(T);

    try {
        for (int k = 0; k < T; k++) {
            int finalK = k;

            executor.execute(() -> {
                long localCount = 0;
                int i = finalK * values.length / T;

                start.countDown();

                try {
                    start.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                while (!stop.get()) {
                    collector.record(values[i]);   // <-- ВЕСЬ ЗАМЕР ЗДЕСЬ
                    localCount++;
                    i++;
                    if (i == values.length) i = finalK * values.length / T;
                }

                ops[finalK] = localCount;

            });
        }

        long t0 = now();
        start.countDown();

        Thread.sleep(millis);
        stop.set(true);

        long t1 = now();
        executor.shutdown();
        if(!executor.awaitTermination(1, TimeUnit.MINUTES))
            System.out.println("executor did not terminate");

        System.out.println("list = " + Arrays.stream(ops).boxed().toList());

        int sum = (int) Arrays.stream(ops).sum();
        System.out.println("ops / sec = " + sum / (int) ((t1 - t0) / 1e9));
        return sum;


    } finally {
        executor.shutdownNow();
    }
}

public static long now() {
    return System.nanoTime();
}

public static long median(long[] values) {
    return Arrays.stream(values).sorted().toArray()[values.length / 2];
}

public static long measurePoint(MetricsCollector collector, int[] values, int T) throws InterruptedException {
    int loops = 5;
    run(new SnapshotMetricsCollector(), values, T, 5000);
    long[] result = new long[loops];

    for (int i = 0; i < loops; i++)
        result[i] = run(collector, values, T, 5000);

    System.out.println("Expected count = " + collector.snapshot().count());
    System.out.println("Actually count = " + Arrays.stream(result).sum());

    return median(result);
}

package lab2;

import lab2.domain.State;
import lab2.domain.ThreadState;
import lab2.util.NamePool;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;

/* Этап 1: планировщик, который ведёт настоящие потоки строго по одному (передача хода через семафоры).
 * Этап 2: обнаружение deadlock в главном цикле и корректное завершение застрявших потоков.
 * Внутреннее устройство (свои классы, семафоры, вспомогательные методы для Shared и SchedLock) придумываете сами. */
public final class Scheduler {
    /** Если true, каждая операция с общими данными добавляет строку в log. Выставляет runner. */
    boolean logging;
    /** Лог прогона: строки "<поток>  <операция>", например "A  lock mu", "A  balance -> 100". */
    final List<String> log = new ArrayList<>();
    /** null, если deadlock не было; иначе "held={a:A, b:B}" (этап 2). */
    String deadlock;
    StringBuilder trace = new StringBuilder();

    private static final ThreadLocal<Scheduler> CURRENT_SCHEDULER = new ThreadLocal<>();
    private static final ThreadLocal<ThreadState> CURRENT_THREAD = new ThreadLocal<>();

    private int number = 0;

    Semaphore semaphore = new Semaphore(0);

    final List<SchedLock> locks = new ArrayList<>();
    boolean aborted;

    NamePool namePool;

    List<ThreadState> threads = new ArrayList<>();
    int last = -1;

    public Scheduler() {
        namePool = new NamePool();
    }

    /** Зарегистрировать поток. Потоки получают буквы A, B, C… в порядке вызова spawn. */
    public void spawn(Runnable body) {
        String name = namePool.acquire();
        ThreadState ts = new ThreadState(name, number++);
        ts.setThread(new Thread(() -> runBody(ts, body), name));
        threads.add(ts);
    }

    private void runBody(ThreadState ts, Runnable body) {
        CURRENT_SCHEDULER.set(this);
        CURRENT_THREAD.set(ts);
        try {
            body.run();
        } catch (AbortRun ignored) {
        } finally {
            ts.setState(State.DONE);
            semaphore.release();
        }
    }

    /** Запустить потоки и вести их по одному: на каждой паузе спросить strategy.pick(enabled, last). */
    public void run(Strategy strategy) {
        for (ThreadState ts : threads) {
            ts.setState(State.RUNNING);
            ts.getThread().start();
            acquire(semaphore);
        }

        try {
            List<Integer> enabled = new ArrayList<>();

            while (true) {
                enabled.clear();
                boolean alive = false;
                for (ThreadState thread : threads) {
                    if (thread.getState() != State.DONE)
                        alive = true;
                    if (thread.getState() == State.WAITING_TURN
                            || thread.getState() == State.WAITING_CONDITION && thread.getCondition().getAsBoolean())
                        enabled.add(thread.getNumber());
                }

                if (!alive) break;
                if (enabled.isEmpty()) {
                    deadlock = heldLocks();
                    break;
                }

                last = strategy.pick(enabled, last);

                var ts = threads.get(last);
                trace.append(ts.getName());
                ts.setState(State.RUNNING);
                ts.getSemaphore().release();
                acquire(semaphore);
            }
        } finally {
            abort();
            for (ThreadState ts : threads) {
                try {
                    ts.getThread().join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private String heldLocks() {
        StringJoiner held = new StringJoiner(", ", "held={", "}");
        for (SchedLock lock : locks)
            if (lock.owner != null)
                held.add(lock.name + ":" + lock.owner.getName());
        return held.toString();
    }

    private void abort() {
        aborted = true;
        for (ThreadState ts : threads)
            if (ts.getState() != State.DONE)
                ts.getSemaphore().release();
    }

    private void pause(ThreadState ts, State waiting) {
        if (aborted) throw new AbortRun();
        ts.setState(waiting);
        ts.setTouchShared(false);
        semaphore.release();
        acquire(ts.getSemaphore());
        if (aborted) throw new AbortRun();
    }

    static ThreadState currentThreadState() {
        return CURRENT_THREAD.get();
    }

    static void beforeOperation() {
        ThreadState ts = CURRENT_THREAD.get();
        if (ts.isTouchShared())
            CURRENT_SCHEDULER.get().pause(ts, State.WAITING_TURN);
    }

    static void beforeOperation(BooleanSupplier ready) {
        ThreadState ts = CURRENT_THREAD.get();
        if (ts.isTouchShared() || !ready.getAsBoolean()) {
            ts.setCondition(ready);
            CURRENT_SCHEDULER.get().pause(ts, State.WAITING_CONDITION);
            ts.setCondition(null);
        }
    }

    static void afterOperation(String op) {
        ThreadState ts = CURRENT_THREAD.get();
        Scheduler s = CURRENT_SCHEDULER.get();
        ts.setTouchShared(true);
        if (s.logging && !s.aborted)
            s.log.add(ts.getName() + "  " + op);
    }

    private static void acquire(Semaphore s) {
        try {
            s.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /** Трасса прогона: буквы потоков, выбранных во всех решениях подряд, например "AAAABB". */
    public String traceString() {
        return trace.toString();
    }

    /** window() из тестовых программ: пауза перед следующей операцией с общими данными обязательна. */
    public static void point() {
        afterOperation("window");
    }

    /** Ждать, пока cond не станет true. До этого поток не попадает в enabled. */
    public static void await(BooleanSupplier cond) {
        beforeOperation(cond);
        afterOperation("await done");
    }

    private static final class AbortRun extends RuntimeException {
    }
}

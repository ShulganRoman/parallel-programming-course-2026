package lab2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/* Стратегии выбора следующего потока.
 * FirstAlive — этап 1, Replay — этап 2, RandomPick — этап 3, Dfs — этап 4 (ограничение bound — этап 5).
 * enabled — номера потоков, которые могут сделать шаг, по возрастанию; last — прошлый выбранный (-1 в начале). */
public final class Strategies {
    private Strategies() { }

    /** Всегда первый поток из enabled. */
    public static final class FirstAlive implements Strategy {
        public int pick(List<Integer> enabled, int last) {
            return enabled.getFirst();
        }
    }

    /** Проигрывает трассу: на i-м решении выбирает поток trace[i] ('A' -> 0, 'B' -> 1). */
    public static final class Replay implements Strategy {
        private int next = 0;
        private final String trace;

        public Replay(String trace) {
            this.trace = trace;
        }

        /** Сбросить позицию в трассе перед прогоном. */
        public void beginRun() {
            next = 0;
        }

        public int pick(List<Integer> enabled, int last) {
            if (next >= trace.length())
                throw new IllegalStateException("трасса " + trace + " кончилась раньше прогона: нужно решение " + (next + 1));

            char threadName = trace.charAt(next++);

            for (int enable : enabled)
                if ((char) ('A' + enable % 26) == threadName)
                    return enable;

            throw new IllegalStateException("решение " + next + " трассы " + trace + ": поток " + threadName
                    + " сейчас не может ходить, enabled=" + enabled);
        }
    }

    /** Случайный поток: enabled.get(rnd.nextInt(enabled.size())), rnd = new java.util.Random(seed). */
    public static final class RandomPick implements Strategy {
        private final Random rnd;

        public RandomPick(long seed) {
            rnd = new Random(seed);
        }

        public int pick(List<Integer> enabled, int last) {
            return enabled.get(rnd.nextInt(enabled.size()));
        }
    }

    /** Stateless DFS по дереву выборов: каждый прогон с нуля, повторяет префикс прошлого и берёт следующий вариант.
     *  bound — максимум вытеснений (этап 5); для dfs runner передаёт Integer.MAX_VALUE. */
    public static final class Dfs implements Strategy {
        private final int bound;
        private final List<Branch> path = new ArrayList<>();
        private int depth;
        private int preemptions;

        public Dfs(int bound) {
            this.bound = bound;
        }

        /** Подготовиться к новому прогону (позиция в пути, счётчик вытеснений). */
        public void beginRun() {
            depth = 0;
            preemptions = 0;
        }

        public int pick(List<Integer> enabled, int last) {
            boolean lastCanRun = enabled.contains(last);

            if (depth == path.size())
                path.add(new Branch(preemptions < bound || !lastCanRun ? List.copyOf(enabled) : List.of(last)));

            Branch branch = path.get(depth++);
            int choice = branch.options.get(branch.index);
            if (!enabled.contains(choice))
                throw new IllegalStateException("прогон не повторил прошлый путь: решение " + depth
                        + ", ждали поток " + (char) ('A' + choice) + ", enabled=" + enabled);

            if (lastCanRun && choice != last)
                preemptions++;
            return choice;
        }

        /** Выбрать следующий путь; false — дерево обойдено. */
        public boolean endRun() {
            while (!path.isEmpty()) {
                Branch branch = path.getLast();
                if (++branch.index < branch.options.size())
                    return true;
                path.removeLast();
            }
            return false;
        }

        private static final class Branch {
            final List<Integer> options;
            int index;

            Branch(List<Integer> options) {
                this.options = options;
            }
        }
    }
}

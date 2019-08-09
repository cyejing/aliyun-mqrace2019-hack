package io.openmessaging;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Born
 */
public class ThroughputRate {

    AtomicInteger throughput = new AtomicInteger(0);
    AtomicBoolean calc = new AtomicBoolean(false);

    private volatile double throughputRate;

    private long interval;
    private volatile long threshold;

    public ThroughputRate(long interval) {
        this.interval = interval;
        this.threshold = System.currentTimeMillis() + interval;
    }

    public int note() {
        checkAndSet();
        return throughput.incrementAndGet();
//        return 0;
    }

    public double getThroughputRate() {
        checkAndSet();
        return throughputRate;
    }
    public void checkAndSet() {
        int i = this.throughput.get();
        long now = System.currentTimeMillis();
        if (now > threshold) {

            if (calc.compareAndSet(false, true)) {
                double t = threshold;
                if (now > t) {
                    this.throughputRate = i * (1000D / (now - t + interval));

                    this.throughput.set(0);
                    this.threshold = now + interval;
                }
                calc.compareAndSet(true, false);
            }

        }
    }

}

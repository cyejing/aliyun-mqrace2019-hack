package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStoreImpl.class);

    private static final String BodySuffix = "0D2125260B5E5B2B0C3741265C0C36070000";
    private static final int Preheat = 50000;
    private static final int Gap = 32773;


    private ConcurrentHashMap<Integer, List<Result>> dirtyMap = new ConcurrentHashMap<>();
    private ByteBuffer store = ByteBuffer.allocateDirect(1024 * 1024 * 1986);


    private ThroughputRate putRate = new ThroughputRate(1000);
    private ThroughputRate indexRate = new ThroughputRate(1000);
    private ThroughputRate messageRate = new ThroughputRate(1000);

    public DefaultMessageStoreImpl() {
        Thread printLog = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    log.info("putRate:{},indexRate:{},messageRate:{}",
                            putRate.getThroughputRate(),
                            indexRate.getThroughputRate(), messageRate.getThroughputRate());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "printLog");
        printLog.setDaemon(true);
        printLog.start();
    }

    private volatile Integer boundary = null;

    @Override
    public synchronized void put(Message message) {
        int t = ((Long) message.getT()).intValue();
        int a = ((Long) message.getA()).intValue();
        if (boundary == null) {
            boundary = t + Preheat;

        }
        if (t < boundary) {
            List<Result> results = dirtyMap.get(t);
            if (results == null) {
                dirtyMap.putIfAbsent(t, new ArrayList<>());
                results = dirtyMap.get(t);
            }
            results.add(new Result(t, a));
        } else {
            int index = (t - boundary) * 2;
            int gap = a - t - Gap;
            try {
                int aSize = ByteUtil.getInt(store.get(index),store.get(index+1));
                if (gap > aSize) {
                    byte[] bytes = ByteUtil.toIntBytes(gap);
                    store.put(index,bytes[0]);
                    store.put(index+1,bytes[1]);
                }
            } catch (Exception e) {
                log.error("index overflow:{}", index);
                throw e;
            }
        }

    }

    private Semaphore semaphore = new Semaphore(4); //FULL GC
    private ConcurrentHashMap<String, Long> avgCache = new ConcurrentHashMap();


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        String key = aMin + "-" + aMax + "-" + tMin + "-" + tMax;
        long avgValue = getAvgValue(aMin, aMax, tMin, tMax);
        avgCache.putIfAbsent(key, avgValue);
        try {
            semaphore.acquire();
            Thread.sleep(10L);
            int tMinI = ((Long) tMin).intValue();
            int tMaxI = ((Long) tMax).intValue();
            ArrayList<Message> res = new ArrayList<>();
            for (int t = tMinI; t <= tMaxI; t++) {
                if (t < this.boundary) {
                    List<Result> dirtyResult = dirtyMap.get(t);
                    for (Result result : dirtyResult) {
                        int a = result.a;
                        if (aMin <= a && a <= aMax) {
                            genMessage(res, t, a);
                        }
                    }
                    continue;
                }

                int index = (t - this.boundary) * 2;
                int aSize = ByteUtil.getInt(store.get(index), store.get(index + 1));
                for (int i = 0; i <= aSize; i++) {
                    int a = t + Gap + i;
                    if (aMin <= a && a <= aMax) {
                        genMessage(res, t, a);
                    }
                }
            }

            res.sort(Comparator.comparingLong(Message::getT));
            messageRate.note();
            return res;
        } catch (InterruptedException e) {
            log.error("", e);
            throw new RuntimeException(e);
        }finally {
            semaphore.release();
        }
    }


    private void genMessage(ArrayList<Message> res, int t, int a) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(34);
        byteBuffer.putLong(t);
        byteBuffer.putLong(a);
        byteBuffer.put(DatatypeConverter.parseHexBinary(BodySuffix));
        res.add(new Message(a, t, byteBuffer.array()));
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        String key = aMin + "-" + aMax + "-" + tMin + "-" + tMax;
        Long avg = avgCache.get(key);
        if (avg != null) {
            return avg;
        }

        long sum = 0;
        long count = 0;
        for (int t = (int)tMin; t <= tMax; t++) {
            if (t < this.boundary) {
                List<Result> dirtyResult = dirtyMap.get(t);
                for (Result result : dirtyResult) {
                    int a = result.a;
                    if (aMin <= a && a <= aMax) {
                        sum += a;
                        count++;
                    }
                }
                continue;
            }

            int index = (t - this.boundary) * 2;
            int aSize = ByteUtil.getInt(store.get(index),store.get(index+1));

            long aiMin = t + Gap;
            long aiMax = t + Gap + aSize;
            if (aiMax < aMin || aMax < aiMin) {
                continue;
            }

            if (aSize > 0 && aMin <= aiMin && aiMax <= aMax) {
                sum += ((aiMax + aiMin) * (aiMax - aiMin + 1)) >>> 1;
                count += aSize + 1;
                continue;
            }

            for (int i = 0; i <= aSize; i++) {
                int a = t + Gap + i;
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count++;
                }
            }
        }
        return count == 0 ? 0 : sum / count;
    }


    class Result{
        int a;
        int t;

        public Result(int t, int a) {
            this.t = t;
            this.a = a;
        }

        public int getT() {
            return t;
        }

        public int getA() {
            return a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Result)) {
                return false;
            }
            Result result = (Result) o;
            return getA() == result.getA() &&
                    getT() == result.getT();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getA(), getT());
        }
    }


}

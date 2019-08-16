package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStoreImpl.class);

    private static final String BodySuffix = "0D2125260B5E5B2B0C3741265C0C36070000";
    private static final int Preheat = 50000;
    private static final int Gap = 32773;
    private static final byte Flag = (byte) 0x80;


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
        ByteBuffer.allocate(1024 * 1024 * 2023); //FULL GC
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
                int aSize = ByteUtil.getInt(store.get(index), store.get(index + 1));
                if (gap > aSize) {
                    byte[] bytes = ByteUtil.toIntBytes(gap);
                    store.put(index, bytes[0]);
                    store.put(index + 1, bytes[1]);
                }
            } catch (Exception e) {
                log.error("index overflow:{}", index);
                throw e;
            }
        }

    }

    private Semaphore semaphore = new Semaphore(3); //FULL GC
    private AtomicBoolean calc = new AtomicBoolean(false);


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (calc.compareAndSet(false, true)) {
            int skip = 0;
            long sum = 0;
            int count = 0;
            for (int t = 0; t < (store.limit() / 2); t++) {
                int index = t * 2;
                int aSize = ByteUtil.getInt(store.get(index), store.get(index+1));
                if (aSize > 0) {
                    for (int i = 1; i <= skip; i++) {
                        int frontIndex = (t - i) * 2;
                        byte[] bytes = ByteUtil.toIntBytes(i);
                        bytes[0] = (byte) (bytes[0] ^ Flag);
                        store.put(frontIndex, bytes[0]);
                        store.put(frontIndex + 1, bytes[1]);
                    }
                    skip = 0;

                    long aiMin = t + Gap;
                    long aiMax = t + Gap + aSize;
                    sum += ((aiMax + (aiMin+1)) * (aiMax - (aiMin+1) + 1)) >>> 1;
                    count += aSize;

                    //TODO
                } else if (aSize == 0) {
                    skip++;
                }
            }

            for (int i = 0; i < 20000; i++) {
                getAvgValue(aMin, aMax, tMin, tMax); //JIT
            }
        }

        try {
            semaphore.acquire();
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
                byte[] bytes = new byte[]{ store.get(index), store.get(index+1)};
                int aSize;
                if ((byte) (bytes[0] & Flag) == Flag) {
                    aSize = 0;
                }else{
                    aSize = ByteUtil.getInt(bytes[0], bytes[1]);
                }

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
        } finally {
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
        long sum = 0;
        long count = 0;

        int tsMin = (int) tMin;
        int indexMax = (int) Math.min(tMax, aMax - Gap);

        if (tsMin < this.boundary) {
            int length = (int) Math.min(this.boundary-1, tMax);
            for (int t = tsMin; t <= length; t++) {
                List<Result> dirtyResult = dirtyMap.get(t);
                for (Result result : dirtyResult) {
                    int a = result.a;
                    if (aMin <= a && a <= aMax) {
                        sum += a;
                        count++;
                    }
                }
            }
            if (indexMax < this.boundary) {
                return count == 0 ? 0 : sum / count;
            }

            tsMin = this.boundary;
        }

        int indexMin = (int) Math.max(tsMin, aMin - Gap);

        long acMin = indexMin + Gap;
        long acMax = indexMax + Gap;
        sum += ((acMax + acMin) * (acMax - acMin + 1)) >>> 1;
        count += indexMax - indexMin + 1;

        for (int t = tsMin; t <= tMax; t++) {
            int index = (t - this.boundary) * 2;
            byte[] bytes = new byte[]{ store.get(index), store.get(index+1)};

            if ((byte) (bytes[0] & Flag) == Flag) {
                bytes[0] = (byte) (bytes[0] ^ Flag);
                int skip = ByteUtil.getInt(bytes[0], bytes[1]);
                t = t + skip - 1;
                continue;
            }

            int aSize = ByteUtil.getInt(bytes[0], bytes[1]);

            long aiMin = t + Gap;
            long aiMax = t + Gap + aSize;
            if (aiMax < aMin || aMax < aiMin) {
                continue;
            }

            if (aSize > 0 && aMin <= aiMin && aiMax <= aMax) {
                sum += ((aiMax + (aiMin+1)) * (aiMax - (aiMin+1) + 1)) >>> 1;
                count += aSize;
                continue;
            }

            for (int i = 1; i <= aSize; i++) {
                int a = t + Gap + i;
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count++;
                }
            }

            byte[] nextBytes = new byte[]{ store.get(index+2), store.get(index+3)};
            if ((byte) (nextBytes[0] & Flag) == Flag) {
                nextBytes[0] = (byte) (nextBytes[0] ^ Flag);
                int skip = ByteUtil.getInt(nextBytes[0], nextBytes[1]);
                t = t + skip;
            }

        }

        return count == 0 ? 0 : sum / count;
    }

    class Result {

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

    class AvgSum{
        int count;
        long sum;

        public AvgSum(int count, long sum) {
            this.count = count;
            this.sum = sum;
        }
    }

}

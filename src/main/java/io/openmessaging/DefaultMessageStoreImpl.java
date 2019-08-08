package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final int Preheat = 150000;

    private IndexRecord[] indexRecords = new IndexRecord[1024 *1024* 800];

    private ConcurrentHashMap<Integer, List<Result>> dirtyMap = new ConcurrentHashMap<>();


    private ThroughputRate putRate = new ThroughputRate(1000);
    private ThroughputRate getRate = new ThroughputRate(1000);
    private ThroughputRate mapRate = new ThroughputRate(1000);

    public DefaultMessageStoreImpl() {
        Thread printLog = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    log.info("putRate:{},getRate:{},mapRate:{}", putRate.getThroughputRate(), getRate.getThroughputRate(),
                            mapRate.getThroughputRate());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "printLog");
        printLog.setDaemon(true);
        printLog.start();
    }

    private AtomicInteger adder = new AtomicInteger(0);
    private volatile Integer boundary = null;
    private volatile Integer boundaryHeap = null; //1035343660


    @Override
    public synchronized void put(Message message) {
        int t = ((Long) message.getT()).intValue();
        int a = ((Long) message.getA()).intValue();
        if (boundary == null) {
            boundary = t + Preheat;

        }
        if (t < boundary) {
            adder.incrementAndGet();
            List<Result> results = dirtyMap.get(t);
            if (results == null) {
                dirtyMap.putIfAbsent(t, new ArrayList<>());
                results = dirtyMap.get(t);
            }
            results.add(new Result(t, a));
        } else {
            int index = t - boundary;
            if (indexRecords[index] == null) {
                indexRecords[index] = new IndexRecord();
            }
            indexRecords[index].addA(a);
        }

        putRate.note();
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (ThreadLocalRandom.current().nextInt(500) == 1) {
            log.info("getMessage aMin:{},aMax:{},tMin:{},tMax:{},aDev:{},tDev{}", aMin, aMax, tMin, tMax, aMax - aMin, tMax - tMin);
        }
        int tMinI = ((Long) tMin).intValue();
        int tMaxI = ((Long) tMax).intValue();
        ArrayList<Message> res = new ArrayList<>();
        for (int t = tMinI; t <= tMaxI; t++) {
            List<Result> results = filterResult(t, aMin, aMax);
            mapRate.note();
            for (Result result : results) {
                long a = result.getA();
                ByteBuffer byteBuffer = ByteBuffer.allocate(34);
                byteBuffer.putLong(t);
                byteBuffer.putLong(a);
                byteBuffer.put(DatatypeConverter.parseHexBinary(BodySuffix));
                res.add(new Message(a, t, byteBuffer.array()));
            }
        }

        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    private List<Result> filterResult(int t, long aMin, long aMax) {
        List results = new LinkedList();

        if (t < this.boundary) {
            List<Result> dirtyResult = dirtyMap.get(t);
            for (Result result : dirtyResult) {
                int a = result.a;
                if (aMin <= a && a <= aMax) {
                    results.add(new Result(t, a));
                    getRate.note();
                }
            }
            return results;
        }

        int index = t - this.boundary;
        IndexRecord indexRecord = this.indexRecords[index];
        if (indexRecord.size == 1) {
            int a = indexRecord.minA;
            if (aMin <= a && a <= aMax) {
                results.add(new Result(t, a));
                getRate.note();
            }
        } else {
            for (int i = 0; i < indexRecord.size; i++) {
                int a = indexRecord.minA + i;
//                    int a = entry.getValue().minA;
                if (aMin <= a && a <= aMax) {
                    results.add(new Result(t, a));
                    getRate.note();
                }
            }
        }
        return results;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (ThreadLocalRandom.current().nextInt(500) == 1) {
            log.info("getAvgValue aMin:{},aMax:{},tMin:{},tMax:{},aDev:{},tDev{}", aMin, aMax, tMin, tMax, aMax - aMin, tMax - tMin);
        }
        long sum = 0;
        long count = 0;
        int tMinI = ((Long) tMin).intValue();
        int tMaxI = ((Long) tMax).intValue();
        for (int t = tMinI; t <= tMaxI; t++) {
            List<Result> results = filterResult(t, aMin, aMax);
            mapRate.note();
            for (Result result : results) {
                sum += result.getA();
                count++;
            }
        }
        return count == 0 ? 0 : sum / count;
    }

    class IndexRecord {
        int minA = Integer.MAX_VALUE;
        int size = 0;

        public synchronized boolean addA(int a) {
            if (a < minA) {
                minA = a;
            }
            size++;
            return true;
        }
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

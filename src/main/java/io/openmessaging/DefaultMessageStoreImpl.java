package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
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

    private ConcurrentHashMap<Integer, IndexRecord> map = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, List<Result>> dirtyMap = new ConcurrentHashMap<>();


    private ThroughputRate putRate = new ThroughputRate(1000);
    private ThroughputRate getRate = new ThroughputRate(1000);

    public DefaultMessageStoreImpl() {
        Thread printLog = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    log.info("putRate:{},getRate:{}", putRate.getThroughputRate(), getRate.getThroughputRate());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "printLog");
        printLog.setDaemon(true);
        printLog.start();
    }

    @Override
    public void put(Message message) {
        int t = ((Long) message.getT()).intValue();
        int a = ((Long) message.getA()).intValue();
        IndexRecord indexRecord = map.get(t);
        if (indexRecord == null) {
            map.putIfAbsent(t, new IndexRecord());
            indexRecord = map.get(t);
        }
        if (!indexRecord.addA(a)) {
            List<Result> results = dirtyMap.get(t);
            if (results == null) {
                ArrayList<Result> list = new ArrayList<>();
                list.add(new Result(t, indexRecord.minA));
                dirtyMap.putIfAbsent(t, list);
                results = dirtyMap.get(t);
            }
            results.add(new Result(t, a));
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
        ArrayList<Message> res = new ArrayList<Message>();
        Map<Integer, IndexRecord> subMap = subMap(tMinI, tMaxI);
        List<Result> results = filterResult(subMap, aMin, aMax);
        for (Result result : results) {
            long t = result.getT();
            long a = result.getA();
            ByteBuffer byteBuffer = ByteBuffer.allocate(34);
            byteBuffer.putLong(t);
            byteBuffer.putLong(a);
            byteBuffer.put(DatatypeConverter.parseHexBinary(BodySuffix));
            res.add(new Message(a, t, byteBuffer.array()));
        }
        res.sort(Comparator.comparingLong(Message::getT));
        return res;
    }

    private Map<Integer, IndexRecord> subMap(int tMin, int tMax) {
        Map<Integer, IndexRecord> result = new HashMap<>();
        for (int i = tMin; i <= tMax; i++) {
            result.put(i, map.get(i));
        }
        return result;
    }

    private List<Result> filterResult(Map<Integer, IndexRecord> subMap,long aMin, long aMax) {
        List results = new LinkedList();
        for (Entry<Integer, IndexRecord> entry : subMap.entrySet()) {
            if (entry.getValue().size == -1) {
                List<Result> dirtyResult = dirtyMap.get(entry.getKey());
                for (Result result : dirtyResult) {
                    int a = result.a;
                    if (aMin <= a && a <= aMax) {
                        results.add(new Result(entry.getKey(), a));
                        getRate.note();
                    }
                }
                continue;
            }

            if (entry.getValue().size == 1) {
                int a = entry.getValue().minA;
                if (aMin <= a && a <= aMax) {
                    results.add(new Result(entry.getKey(), a));
                    getRate.note();
                }
            }else{
                for (int i = 0; i < entry.getValue().size; i++) {
                    int a = entry.getValue().minA + i;
//                    int a = entry.getValue().minA;
                    if (aMin <= a && a <= aMax) {
                        results.add(new Result(entry.getKey(), a));
                        getRate.note();
                    }
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
        Map<Integer, IndexRecord> subMap = subMap(tMinI, tMaxI);
        List<Result> results = filterResult(subMap, aMin, aMax);
        for (Result result : results) {
            sum += result.getA();
            count++;
        }
        return count == 0 ? 0 : sum / count;
    }

    class IndexRecord {
        int minA = Integer.MAX_VALUE;
        int size = 0;

        public synchronized boolean addA(int a) {
            if (size == -1 || (minA != Integer.MAX_VALUE && Math.abs(a - minA) > 50)) {
                size = -1; //dirty
                return false;
            } else {
                if (a < minA) {
                    minA = a;
                }
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

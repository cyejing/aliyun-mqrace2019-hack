
package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class DemoTester {
    private static Random globalRandom = new Random();
    private static int startNum;
    private static byte[] bodyBytes;
    private static final int BODY_SIZE = 34;
    private static int A_DELTA;
    private static int SMALL_T_SIZE;
    private static int LARGE_T_SIZE;
    private static int REPEAT_1_MAX;
    private static int REPEAT_2_MAX;
    private static int REPEAT_3_MAX;
    private static int REPEAT_3_MASK;
    private static int BOUND1_NUM;
    private static int BOUND2_NUM;
    private static int BOUND3_NUM;
    private static int BOUND4_NUM;
    private static int SMALL_RANGE_NUM;
    private static int LARGE_RANGE_NUM;
    private static double factor3;
    private static double factor4;
    private static int BOUND_1;
    private static int BOUND_2;
    private static int BOUND_3;
    private static int BOUND_4;
    private static ArrayList<ArrayList<Message>> bound1Msgs;
    private static LinkedBlockingQueue<OneResult> msgResultQueue;
    private static LinkedBlockingQueue<OneResult> valueResultQueue;
    private static final int maxSendTime = 1800000;
    private static final int maxCheckTime = 1800000;


    static {
        startNum = globalRandom.nextInt(10000000) + 500;
        bodyBytes = new byte[]{13, 33, 37, 38, 11, 94, 91, 43, 12, 55, 65, 38, 92, 12, 54, 7};
        A_DELTA = 32773;
        SMALL_T_SIZE = 4328;
        LARGE_T_SIZE = 223291;
        REPEAT_1_MAX = 20;
        REPEAT_2_MAX = 8;
        REPEAT_3_MAX = 2088;
        REPEAT_3_MASK = 3125;
        BOUND1_NUM = 5000;
//        BOUND2_NUM = 700040210;
//        BOUND3_NUM = 700592209;
//        BOUND4_NUM = 703893211;

        BOUND2_NUM = 7040210;
        BOUND3_NUM = 7592209;
        BOUND4_NUM = 7893211;

        SMALL_RANGE_NUM = 932;
        LARGE_RANGE_NUM = 16930;
        factor3 = (double)REPEAT_3_MAX / (double)REPEAT_3_MASK + 1.0D;
        factor4 = (double)REPEAT_3_MAX / (double)REPEAT_3_MASK + 2.0D;
        BOUND_1 = BOUND1_NUM + startNum;
        BOUND_2 = (BOUND2_NUM >>> 1) + BOUND_1;
        BOUND_3 = (int)((double)BOUND3_NUM / factor3 + (double)BOUND_2);
        BOUND_4 = (int)((double)BOUND4_NUM / factor4 + (double)BOUND_3);
        bound1Msgs = new ArrayList(BOUND1_NUM);
        msgResultQueue = new LinkedBlockingQueue();
        valueResultQueue = new LinkedBlockingQueue();
    }

    public DemoTester() {
    }

    private static void error(String errorSting) {
        System.err.println(errorSting);
    }

    public static void main(String[] args) throws Exception {
        int msgNum = BOUND_4;
        int sendThreadNum = 12;
        int checkMsgThreadNum = 6;
        int getMsgThreadNum = 8;
        int getValueThreadNum = 12;
        MessageStore messageStore = null;

        System.out.println("race2019-d");
        System.out.println(BOUND_1);
        System.out.println(BOUND_2);
        System.out.println(BOUND_3);
        System.out.println(BOUND_4);
        System.out.println(BOUND1_NUM);

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultMessageStoreImpl");
            messageStore = (MessageStore)queueStoreClass.newInstance();
        } catch (Throwable var32) {
            var32.printStackTrace(System.out);
            error(var32.toString());
            System.exit(-1);
        }

        for(int i = startNum; i <= BOUND_1; ++i) {
            bound1Msgs.add(new ArrayList(i % 10 == 0 ? REPEAT_1_MAX + 1 : 1));
        }

        AtomicLong sendCounter = new AtomicLong(0L);
        AtomicLong tIndexCounter = new AtomicLong((long)startNum);
        Thread[] sends = new Thread[sendThreadNum];
        long sendStart = System.currentTimeMillis();

        for(int i = 0; i < sends.length; ++i) {
            sends[i] = new Thread(new Producer(messageStore, msgNum, sendCounter, tIndexCounter));
            sends[i].setName("Producer-Thread-" + i);
        }

        Timer producerTimer = new Timer(true);
        producerTimer.schedule(new TimerTask() {
            public void run() {
                System.out.println("MessageStore.put超时");
                DemoTester.error("MessageStore.put超时");
                System.exit(-1);
            }
        }, 1800000L);

        int i;
        for(i = 0; i < sends.length; ++i) {
            sends[i].start();
        }

        for(i = 0; i < sends.length; ++i) {
            sends[i].join();
        }

        long sendSend = System.currentTimeMillis();
        producerTimer.cancel();

        System.out.printf("Send Score:%d\n", (sendCounter.get() / (sendSend- sendStart)));

        AtomicLong messageCheckNum = new AtomicLong(0L);
        Thread[] msgChecks = new Thread[checkMsgThreadNum];

        for(i = 0; i < msgChecks.length; ++i) {
            msgChecks[i] = new Thread(new MessageChecker(messageCheckNum,messageStore));
            msgChecks[i].setName("MessageChecker-" + i);
        }

        for(i = 0; i < msgChecks.length; ++i) {
            msgChecks[i].start();
        }

        Timer msgCheckTimer = new Timer(true);
        msgCheckTimer.schedule(new TimerTask() {
            public void run() {
                System.out.println("查询阶段超时");
                DemoTester.error("查询阶段超时");
                System.exit(-1);
            }
        }, 1800000L);
        long msgCheckStart = System.currentTimeMillis();
        rangerPicker(messageStore, getMsgThreadNum, 1800000, msgCheckStart, true);
        long msgCheckEnd = System.currentTimeMillis();
        msgCheckTimer.cancel();


        for(i = 0; i < msgChecks.length; ++i) {
            msgResultQueue.add(new OneResult(-1, 0L, 0L, 0L, 0L, (List)null));
        }

        for(i = 0; i < msgChecks.length; ++i) {
            msgChecks[i].join();
        }
        System.out.printf("Message Check Score:%d\n", (messageCheckNum.get() / (msgCheckEnd - msgCheckStart)));


        AtomicLong valueCheckNum = new AtomicLong(0L);
        Thread[] valueChecks = new Thread[checkMsgThreadNum];

        for(i = 0; i < valueChecks.length; ++i) {
            valueChecks[i] = new Thread(new ValueChecker(valueCheckNum,messageStore));
            valueChecks[i].setName("ValueChecker-" + i);
        }

        for(i = 0; i < valueChecks.length; ++i) {
            valueChecks[i].start();
        }

        Timer valueCheckTimer = new Timer(true);
        valueCheckTimer.schedule(new TimerTask() {
            public void run() {
                System.out.println("查询阶段超时");
                DemoTester.error("查询阶段超时");
                System.exit(-1);
            }
        }, 1800000L - (msgCheckEnd - msgCheckStart));
        long valueCheckStart = System.currentTimeMillis();
        rangerPicker(messageStore, getValueThreadNum, 1800000, valueCheckStart, false);
        long valueCheckEnd = System.currentTimeMillis();
        valueCheckTimer.cancel();

        for(i = 0; i < valueChecks.length; ++i) {
            valueResultQueue.add(new OneResult(-1, 0L, 0L, 0L, 0L, (List)null));
        }

        for(i = 0; i < valueChecks.length; ++i) {
            valueChecks[i].join();
        }
        System.out.printf("Value Check Score:%d\n", (valueCheckNum.get() / (valueCheckEnd - valueCheckStart)));

        try {
            Thread.sleep(3000L);
        } catch (Throwable var31) {
            ;
        }

        error("S|" + sendCounter.get() + "|" + (sendSend - sendStart) + "|" + messageCheckNum.get() + "|" + (msgCheckEnd - msgCheckStart) + "|" + valueCheckNum.get() + "|" + (valueCheckEnd - valueCheckStart));

        System.out.printf("Send Score:%d\n", (sendCounter.get() / (sendSend- sendStart)));
        System.out.printf("Message Check Score:%d\n", (messageCheckNum.get() / (msgCheckEnd - msgCheckStart)));
        System.out.printf("Value Check Score:%d\n", (valueCheckNum.get() / (valueCheckEnd - valueCheckStart)));
        System.out.printf("Total Score:%d\n", (sendCounter.get() / (sendSend- sendStart) + messageCheckNum.get() / (msgCheckEnd - msgCheckStart) + valueCheckNum.get() / (valueCheckEnd - valueCheckStart)));


        try {
            Thread.sleep(1000L);
        } catch (Throwable var30) {
            ;
        }

        System.exit(0);
    }

    private static void rangerPicker(MessageStore messageStore, int getMsgNum, int maxCheckTime, long msgCheckStart, boolean pickMessageOrValue) throws InterruptedException {
        Thread[] getMessageThreads = new Thread[getMsgNum];
        long maxMsgCheckTimes = msgCheckStart + (long)maxCheckTime;
        int tNum = 0;
        int fewThreads = 4;

        int i;
        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, startNum, BOUND_1, maxMsgCheckTimes, BOUND1_NUM / 10, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].join();
        }

        int bound2CheckNum = 658;

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_1 + 1, BOUND_2, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].join();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_1 + 1, BOUND_2, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, true));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].join();
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_2 + 1, BOUND_3, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].join();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_2 + 1, BOUND_3, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].join();
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_3 + 1, BOUND_4, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < getMessageThreads.length; ++i) {
            getMessageThreads[i].join();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i] = new Thread(new RangePicker(messageStore, BOUND_3 + 1, BOUND_4, maxMsgCheckTimes, bound2CheckNum, pickMessageOrValue, false));
            getMessageThreads[i].setName("RangePicker-" + tNum++);
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].start();
        }

        for(i = 0; i < fewThreads; ++i) {
            getMessageThreads[i].join();
        }

    }

    private static Message generateMessageBody(Message message, ByteBuffer bb) {
        bb.putLong(message.getT());
        bb.putLong(message.getA());
        bb.put(bodyBytes);
        message.setBody(bb.array());
        return message;
    }

    private static boolean checkMessageBody(Message message, long idx) {
        if (message.getA() <= idx + 10L && message.getA() >= idx) {
            ByteBuffer bb = ByteBuffer.wrap(message.getBody());
            return bb.getLong() == message.getT() && bb.getLong() == message.getA();
        } else {
            return true;
        }
    }

    private static boolean isInARange(OneResult rst, Message msg) {
        return msg.getA() >= rst.aMin && msg.getA() <= rst.aMax;
    }

    private static void printStatus(OneResult rst) {
        System.err.println("startNum:" + startNum + " BOUND_1:" + BOUND_1 + " BOUND_2:" + BOUND_2 + " BOUND3_NUM:" + BOUND3_NUM + " BOUND4_NUM:" + BOUND4_NUM);
    }


    static class ValueChecker implements Runnable {
        private AtomicLong numCounter;
        private long sum = 0L;
        private long count = 0L;
        private MessageStore messageStore;

        public ValueChecker(AtomicLong numCounter, MessageStore messageStore) {
            this.messageStore = messageStore;
            this.numCounter = numCounter;
        }

        private void checkError(String tag, String reason) {
            System.out.println("getAvgValue 错误. " + reason);
            DemoTester.error("getAvgValue 错误. " + reason);
            System.exit(-1);
        }

        private void computeRealValue(OneResult rst, int repeatTimes, int mask, boolean needTotal, long s) {
            for(int i = needTotal ? 0 : 1; i <= repeatTimes; ++i) {
                long aMax;
                long aMin;
                if (i == 0) {
                    aMax = Math.max(rst.tMin + (long) DemoTester.A_DELTA, rst.aMin);
                    aMin = Math.min(rst.tMax + (long) DemoTester.A_DELTA, rst.aMax);
                    if (aMax >= aMin) {
                        DemoTester.printStatus(rst);
                        this.checkError("U", "computeRealValue aMin >= aMax");
                    }

                    this.sum += (aMax + aMin) * (aMin - aMax + 1L) >>> 1;
                    this.count += aMin - aMax + 1L;
                } else {
                    long tMin = Math.max(rst.tMin, rst.aMin - (long) DemoTester.A_DELTA);
                    long aMin_Mask;
                    if (mask == DemoTester.REPEAT_2_MAX) {
                        aMin_Mask = (tMin & 7L) == 0L ? tMin - (long) DemoTester.REPEAT_2_MAX + (long) DemoTester.A_DELTA + (long)i : (tMin & -8L) + (long) DemoTester.A_DELTA + (long)i;
                    } else {
                        aMin_Mask = tMin / (long)mask * (long)mask + (long) DemoTester.A_DELTA + (long)i;
                    }

                    aMin = aMin_Mask >= rst.aMin && aMin_Mask - (long) DemoTester.A_DELTA - (long)i >= rst.tMin ? aMin_Mask : aMin_Mask + (long)mask;
                    if (aMin <= rst.aMax) {
                        long tMax = Math.min(rst.tMax, rst.aMax - (long) DemoTester.A_DELTA);
                        long aMax_Mask;
                        if (mask == DemoTester.REPEAT_2_MAX) {
                            aMax_Mask = (tMax & -8L) + (long) DemoTester.A_DELTA + (long)i;
                        } else {
                            aMax_Mask = tMax / (long)mask * (long)mask + (long) DemoTester.A_DELTA + (long)i;
                        }

                        aMax = aMax_Mask <= rst.aMax && aMax_Mask - (long) DemoTester.A_DELTA - (long)i <= rst.tMax ? aMax_Mask : aMax_Mask - (long)mask;
                        if (aMax >= rst.aMin && aMax >= aMin) {
                            long ct = 0L;
                            if (mask == DemoTester.REPEAT_2_MAX) {
                                ct = (aMax - aMin >>> 3) + 1L;
                            } else {
                                ct = (aMax - aMin) / (long)mask + 1L;
                            }

                            this.sum += (aMin + aMax) * ct >>> 1;
                            this.count += ct;
                        }
                    }
                }
            }

        }

        public void run() {
            OneResult rst = null;

            while(true) {
                while(true) {
                    try {
                        rst = (OneResult) DemoTester.valueResultQueue.take();
                        if (rst.bound == -1) {
                            return;
                        }

                        this.sum = 0L;
                        this.count = 0L;
                        long tMin;
                        if (rst.bound != DemoTester.BOUND_1) {
                            if (rst.bound == DemoTester.BOUND_2) {
                                this.computeRealValue(rst, DemoTester.REPEAT_2_MAX, DemoTester.REPEAT_2_MAX, true, 0L);
                            } else if (rst.bound == DemoTester.BOUND_3) {
                                this.computeRealValue(rst, DemoTester.REPEAT_3_MAX, DemoTester.REPEAT_3_MASK, true, 0L);
                            } else {
                                this.computeRealValue(rst, DemoTester.REPEAT_2_MAX, DemoTester.REPEAT_2_MAX, true, 0L);
                                tMin = Math.min(rst.aMin - (long) DemoTester.A_DELTA, rst.tMin);
                                long tMin_t = tMin / (long) DemoTester.REPEAT_3_MASK * (long) DemoTester.REPEAT_3_MASK;

                                for(long tIndex = tMin_t; tIndex <= rst.tMax; tIndex += (long) DemoTester.REPEAT_3_MASK) {
                                    long aMin = tIndex + (long) DemoTester.A_DELTA + 1L;
                                    long aMax = tIndex + (long) DemoTester.A_DELTA + (long) DemoTester.REPEAT_3_MAX;
                                    if (tIndex >= rst.tMin && tIndex <= rst.tMax && aMin <= rst.aMax && aMax >= rst.aMin && (tIndex & 7L) != 0L) {
                                        aMin = Math.max(aMin, rst.aMin);
                                        aMax = Math.min(aMax, rst.aMax);
                                        if (aMin > aMax) {
                                            DemoTester.printStatus(rst);
                                            this.checkError("U", "aMin > aMax in B4");
                                        }

                                        this.sum += (aMin + aMax) * (aMax - aMin + 1L) >>> 1;
                                        this.count += aMax - aMin + 1L;
                                    }
                                }
                            }
                        } else {
                            for(int tIndex = (int)rst.tMin; (long)tIndex <= rst.tMax; ++tIndex) {
                                ArrayList<Message> list = (ArrayList) DemoTester.bound1Msgs.get(tIndex - DemoTester.startNum);
                                if (list.size() == 1) {
                                    if (DemoTester.isInARange(rst, (Message)list.get(0))) {
                                        this.sum += ((Message)list.get(0)).getA();
                                        ++this.count;
                                    }
                                } else {
                                    Iterator var4 = list.iterator();

                                    while(var4.hasNext()) {
                                        Message msg = (Message)var4.next();
                                        if (DemoTester.isInARange(rst, msg)) {
                                            this.sum += msg.getA();
                                            ++this.count;
                                        }
                                    }
                                }
                            }
                        }

                        tMin = this.count == 0L ? 0L : this.sum / this.count;
                        if (tMin != rst.value) {
                            this.checkError("E", "getAvgValue is not correct");
                        }

                        this.numCounter.getAndAdd(this.count);
                    } catch (Throwable var12) {
                        var12.printStackTrace(System.out);
                        DemoTester.error(var12.getMessage());
                        System.exit(-1);
                    }
                }
            }
        }
    }

    static class MessageChecker implements Runnable {
        private AtomicLong numCounter;
        private ArrayList<Message> repeatList1;
        private ArrayList<Message> repeatList1_2;
        private ArrayList<Message> repeatList2;
        private ArrayList<Message> repeatList3;
        private Comparator<Message> cmp;
        private MessageStore messageStore;

        public MessageChecker(AtomicLong numCounter,MessageStore messageStore) {
            this.repeatList1 = new ArrayList(DemoTester.REPEAT_1_MAX + 1);
            this.repeatList1_2 = new ArrayList(DemoTester.REPEAT_1_MAX + 1);
            this.repeatList2 = null;
            this.repeatList3 = null;
            this.cmp = new Comparator<Message>() {
                public int compare(Message o1, Message o2) {
                    return (int)(o1.getA() - o2.getA());
                }
            };
            this.numCounter = numCounter;
            this.messageStore = messageStore;
        }

        private void checkError(String reason, OneResult rst) {
            System.out.println("getMessage 错误. " + reason + ", aMin:" + rst.aMin + ",aMax:" + rst.aMax + ",tMin:" + rst.tMin + ",tMax:" + rst.tMax);
            DemoTester.error("getMessage 错误. " + reason);
            System.exit(-1);
        }

        private void checkResultSpec(OneResult rst) {
            int idx = 0;
            int mask = -1;
            int repeat = -1;
            int mask2 = -1;
            int repeat2 = -1;
            ArrayList<Message> rlist = null;
            if (rst.bound == DemoTester.BOUND_2) {
                repeat = DemoTester.REPEAT_2_MAX;
                mask = repeat - 1;
                if (this.repeatList2 == null) {
                    this.repeatList2 = new ArrayList(DemoTester.REPEAT_2_MAX + 1);
                }
            } else if (rst.bound == DemoTester.BOUND_3) {
                repeat2 = DemoTester.REPEAT_3_MAX;
                mask2 = DemoTester.REPEAT_3_MASK;
                if (this.repeatList3 == null) {
                    this.repeatList3 = new ArrayList(DemoTester.REPEAT_3_MAX + 1);
                }
            } else if (rst.bound == DemoTester.BOUND_4) {
                repeat = DemoTester.REPEAT_2_MAX;
                mask = repeat - 1;
                repeat2 = DemoTester.REPEAT_3_MAX;
                mask2 = DemoTester.REPEAT_3_MASK;
            }

            int real_repeat = 1;
            long tMin = 0L;
            if (rst.tMin < rst.aMin - (long) DemoTester.A_DELTA) {
                long tMin_t;
                if (rst.bound != DemoTester.BOUND_4) {
                    tMin = rst.aMin - (long) DemoTester.A_DELTA;
                    tMin_t = 0L;
                    if (rst.bound == DemoTester.BOUND_2) {
                        tMin_t = (tMin & 7L) == 0L ? tMin - 8L : tMin & -8L;
                    } else {
                        tMin_t = tMin / (long) DemoTester.REPEAT_3_MASK * (long) DemoTester.REPEAT_3_MASK;
                    }

                    tMin = Math.max(rst.tMin, tMin_t);
                } else {
                    tMin = rst.aMin - (long) DemoTester.A_DELTA;
                    tMin_t = (tMin & 7L) == 0L ? tMin - 8L : tMin & -8L;
                    long tMin_t2 = tMin / (long) DemoTester.REPEAT_3_MASK * (long) DemoTester.REPEAT_3_MASK;
                    if (tMin_t >= rst.tMin && tMin_t2 >= rst.tMin) {
                        tMin = Math.min(tMin_t, tMin_t2);
                    } else {
                        tMin = Math.max(rst.tMin, Math.max(tMin_t, tMin_t2));
                    }
                }
            } else {
                tMin = rst.tMin;
            }

            for(int tIndex = (int)tMin; (long)tIndex <= rst.tMax; ++tIndex) {
                long aMax = 0L;
                long aMin = (long)(tIndex + DemoTester.A_DELTA);
                if (mask != -1 && (tIndex & mask) == 0) {
                    real_repeat = repeat;
                    rlist = this.repeatList2;
                    aMax = (long)(tIndex + DemoTester.A_DELTA + repeat);
                } else if (mask2 != -1 && tIndex % mask2 == 0) {
                    real_repeat = repeat2;
                    rlist = this.repeatList3;
                    aMax = (long)(tIndex + DemoTester.A_DELTA + repeat2);
                } else {
                    real_repeat = -1;
                    rlist = null;
                    aMax = (long)(tIndex + DemoTester.A_DELTA);
                }

                if (aMin > rst.aMax) {
                    break;
                }

                if (aMax >= rst.aMin) {
                    if (real_repeat != -1) {
                        for(boolean var16 = false; idx < rst.msgs.size(); ++idx) {
                            Message msg = (Message)rst.msgs.get(idx);
                            if (msg.getT() != (long)tIndex) {
                                break;
                            }

                            rlist.add(rst.msgs.get(idx));
                        }

                        long rMin = Math.max(rst.aMin, aMin);
                        long rMax = Math.min(rst.aMax, aMax);
                        if (rMin > rMax) {
                            if (rlist.size() != 0) {
                                this.checkError("返回结果太多", rst);
                            }
                        } else {
                            if ((long)rlist.size() != rMax - rMin + 1L) {
                                this.checkError("结果size错误", rst);
                            }

                            Collections.sort(rlist, this.cmp);

                            for(Iterator var21 = rlist.iterator(); var21.hasNext(); ++rMin) {
                                Message msg = (Message)var21.next();
                                if (msg.getA() != rMin || !DemoTester.checkMessageBody(msg, rst.checkIdx)) {
                                    this.checkError("返回结果错误", rst);
                                }
                            }

                            rlist.clear();
                        }
                    } else {
                        if (rst.msgs.size() < idx + 1) {
                            this.checkError("返回结果错误", rst);
                        }

                        Message msg = (Message)rst.msgs.get(idx);
                        if (msg.getT() != (long)tIndex || msg.getA() != (long)(tIndex + DemoTester.A_DELTA) || !DemoTester
                                .checkMessageBody(msg, rst.checkIdx)) {
                            this.checkError("返回结果错误", rst);
                        }

                        ++idx;
                    }
                }
            }

            if (idx != rst.msgs.size()) {
                this.checkError("结果size错误", rst);
            }

        }

        public void run() {
            OneResult rst = null;

            while(true) {
                while(true) {
                    try {
                        rst = (OneResult) DemoTester.msgResultQueue.take();
                        if (rst.bound == -1) {
                            return;
                        }

                        if (rst.msgs == null) {
                            rst.msgs = new LinkedList();
                        }

                        if (rst.bound != DemoTester.BOUND_1) {
                            this.checkResultSpec(rst);
                        } else {
                            int idx = 0;
                            int tIndex = (int)rst.tMin;

                            while(true) {
                                if ((long)tIndex > rst.tMax) {
                                    if (idx != rst.msgs.size()) {
                                        this.checkError("返回结果size错误", rst);
                                    }
                                    break;
                                }

                                ArrayList<Message> list = (ArrayList) DemoTester.bound1Msgs.get(tIndex - DemoTester.startNum);
                                if (list.size() == 1) {
                                    if (DemoTester.isInARange(rst, (Message)list.get(0))) {
                                        if (rst.msgs.size() < idx + 1) {
                                            this.checkError("结果错误", rst);
                                        }

                                        if (!((Message)list.get(0)).equals(rst.msgs.get(idx))) {
                                            this.checkError("结果错误", rst);
                                        }

                                        ++idx;
                                    }
                                } else {
                                    Iterator var5 = list.iterator();

                                    while(var5.hasNext()) {
                                        Message msg = (Message)var5.next();
                                        if (DemoTester.isInARange(rst, msg)) {
                                            this.repeatList1.add(msg);
                                        }
                                    }

                                    while(idx < rst.msgs.size() && ((Message)rst.msgs.get(idx)).getT() == (long)tIndex) {
                                        this.repeatList1_2.add(rst.msgs.get(idx));
                                        ++idx;
                                    }

                                    Collections.sort(this.repeatList1, this.cmp);
                                    Collections.sort(this.repeatList1_2, this.cmp);
                                    if (!this.repeatList1.equals(this.repeatList1_2)) {
                                        this.checkError("结果错误", rst);
                                    }

                                    this.repeatList1.clear();
                                    this.repeatList1_2.clear();
                                }

                                ++tIndex;
                            }
                        }

                        this.numCounter.getAndAdd((long)rst.msgs.size());
                        rst.msgs = null;
                    } catch (Throwable var7) {
                        var7.printStackTrace(System.out);
                        DemoTester.error(var7.getMessage());
                        System.exit(-1);
                    }
                }
            }
        }
    }

    static class OneResult {
        int bound;
        long tMin;
        long tMax;
        long aMin;
        long aMax;
        List<Message> msgs;
        long value;
        long checkIdx;

        public OneResult(int bound, long tMin, long tMax, long aMin, long aMax, List<Message> msgs) {
            this.bound = bound;
            this.tMin = tMin;
            this.tMax = tMax;
            this.aMin = aMin;
            this.aMax = aMax;
            this.msgs = msgs;
            this.checkIdx = (aMax - aMin >>> 1) + aMin;
        }

        public OneResult(int bound, long tMin, long tMax, long aMin, long aMax, long value) {
            this.bound = bound;
            this.tMin = tMin;
            this.tMax = tMax;
            this.aMin = aMin;
            this.aMax = aMax;
            this.value = value;
        }
    }

    private static class RangePicker implements Runnable {
        private int checkMin;
        private int checkMax;
        private long maxTimeStamp;
        private int checkNum;
        private boolean pickMessageOrValue;
        private MessageStore messageStore;
        private boolean hugeMode;

        public RangePicker(MessageStore messageStore, int checkMin, int checkMax, long maxTimeStamp, int checkNum, boolean pickMessageOrValue, boolean hugeMode) {
            this.messageStore = messageStore;
            this.checkMin = checkMin;
            this.checkMax = checkMax;
            this.maxTimeStamp = maxTimeStamp;
            this.checkNum = checkNum;
            this.pickMessageOrValue = pickMessageOrValue;
            this.hugeMode = hugeMode;
        }

        public void run() {
            try{

            int smallTNum;
            int half;
            long tMin;
            long tMax;
            long aMin;
            if (this.checkMax == DemoTester.BOUND_1) {
                smallTNum = this.checkNum >>> 1;

                for(half = 0; half < smallTNum; ++half) {
                    tMin = ThreadLocalRandom.current().nextLong((long)this.checkMin, (long)this.checkMax);
                    tMax = tMin + 100L >= (long)this.checkMax ? (long)this.checkMax : ThreadLocalRandom.current().nextLong(tMin, (long)this.checkMax);
                    aMin = tMin == (long)this.checkMin ? tMin : ThreadLocalRandom.current().nextLong((long)this.checkMin, tMin);
                    aMin = tMax == (long)this.checkMax ? tMax : ThreadLocalRandom.current().nextLong(tMax, (long)this.checkMax);
                    this.getResult(tMin, tMax, aMin, aMin);
                }

                for(half = 0; half < smallTNum; ++half) {
                    tMin = ThreadLocalRandom.current().nextLong((long)this.checkMin, (long)this.checkMax);
                    tMax = tMin + 100L >= (long)this.checkMax ? (long)this.checkMax : ThreadLocalRandom.current().nextLong(tMin, (long)this.checkMax);
                    aMin = tMin == tMax ? tMin : ThreadLocalRandom.current().nextLong(tMin, tMax);
                    aMin = aMin == tMax ? tMax : ThreadLocalRandom.current().nextLong(aMin, tMax);
                    this.getResult(tMin, tMax, aMin, aMin);
                }
            } else if (!this.hugeMode) {
                smallTNum = this.checkNum * 5 / 6;
                half = smallTNum >>> 1;

                int delta;
                long aMax;
                int i;
                for(i = 0; i < smallTNum; ++i) {
                    tMin = ThreadLocalRandom.current().nextLong((long)this.checkMin, (long)(this.checkMax - DemoTester.SMALL_T_SIZE - i));
                    tMax = tMin + (long) DemoTester.SMALL_T_SIZE + (long)i;
                    delta = i % DemoTester.SMALL_RANGE_NUM;
                    aMin = 0L;
                    aMax = 0L;
                    if (i <= half) {
                        aMin = tMin + (long) DemoTester.A_DELTA - (long) DemoTester.SMALL_RANGE_NUM - (long)delta;
                        aMax = tMax + (long) DemoTester.A_DELTA + (long) DemoTester.SMALL_RANGE_NUM + (long)delta;
                    } else {
                        aMin = tMin + (long) DemoTester.A_DELTA + (long) DemoTester.SMALL_RANGE_NUM + (long)delta;
                        aMax = tMax + (long) DemoTester.A_DELTA - (long) DemoTester.SMALL_RANGE_NUM - (long)delta;
                    }

                    this.getResult(tMin, tMax, aMin, aMax);
                }

                smallTNum = this.checkNum / 12;

                for(i = 0; i < smallTNum; ++i) {
                    tMin = ThreadLocalRandom.current().nextLong((long)this.checkMin, (long)(this.checkMax - DemoTester.LARGE_T_SIZE - i));
                    tMax = tMin + (long) DemoTester.LARGE_T_SIZE + (long)i;
                    delta = i % DemoTester.LARGE_RANGE_NUM;
                    aMin = 0L;
                    aMax = 0L;
                    aMin = tMin + (long) DemoTester.A_DELTA + (long) DemoTester.LARGE_RANGE_NUM + (long)delta;
                    aMax = tMax + (long) DemoTester.A_DELTA - (long) DemoTester.LARGE_RANGE_NUM - (long)delta;
                    this.getResult(tMin, tMax, aMin, aMax);
                }
            } else {
                smallTNum = this.checkNum >>> 4;

                for(half = 0; half < smallTNum; ++half) {
                    tMin = ThreadLocalRandom.current().nextLong((long)this.checkMin, (long)(this.checkMax - DemoTester.LARGE_T_SIZE - half));
                    tMax = tMin + (long) DemoTester.LARGE_T_SIZE + (long)half;
                    int delta = half % DemoTester.LARGE_RANGE_NUM;
                    aMin = 0L;
                    long aMax = 0L;
                    aMin = tMin + (long) DemoTester.A_DELTA - (long) DemoTester.LARGE_RANGE_NUM - (long)delta;
                    aMax = tMax + (long) DemoTester.A_DELTA + (long) DemoTester.LARGE_RANGE_NUM + (long)delta;
                    this.getResult(tMin, tMax, aMin, aMax);
                }
            }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        private void getResult(long tMin, long tMax, long aMin, long aMax) {
            if (this.pickMessageOrValue) {
                List<Message> msgs = this.messageStore.getMessage(aMin, aMax, tMin, tMax);
                DemoTester.msgResultQueue.add(new OneResult(this.checkMax, tMin, tMax, aMin, aMax, msgs));
            } else {
                long value = this.messageStore.getAvgValue(aMin, aMax, tMin, tMax);
                DemoTester.valueResultQueue.add(new OneResult(this.checkMax, tMin, tMax, aMin, aMax, value));
            }

        }
    }

    static class Producer implements Runnable {
        private AtomicLong sendCounter;
        private AtomicLong tIndexCounter;
        private long maxMsgNum;
        private MessageStore messageStore;

        public Producer(MessageStore messageStore, int maxMsgNum, AtomicLong sendCounter, AtomicLong tIndexCounter) {
            this.sendCounter = sendCounter;
            this.tIndexCounter = tIndexCounter;
            this.maxMsgNum = (long)maxMsgNum;
            this.messageStore = messageStore;
        }

        public void run() {
            long t;
            while((t = this.tIndexCounter.getAndIncrement()) <= (long) DemoTester.BOUND_4) {
                try {
                    int repeatNum = 0;
                    if (t <= (long) DemoTester.BOUND_1) {
                        long a = (long)ThreadLocalRandom.current().nextInt((int)t - 500, DemoTester.startNum + 10000);
                        Message msg = DemoTester.generateMessageBody(new Message(a, t, (byte[])null), ByteBuffer.allocate(34));
                        this.messageStore.put(msg);
                        ArrayList<Message> msgList = (ArrayList) DemoTester.bound1Msgs.get((int)(t - (long) DemoTester.startNum));
                        msgList.add(msg);
                        if (t % 10L == 0L) {
                            repeatNum = ThreadLocalRandom.current().nextInt(1, DemoTester.REPEAT_1_MAX + 1);

                            for(int i = 0; i < repeatNum; ++i) {
                                a = (long)ThreadLocalRandom.current().nextInt((int)t - 500, DemoTester.startNum + 10000);
                                msg = DemoTester.generateMessageBody(new Message(a, t, (byte[])null), ByteBuffer.allocate(34));
                                this.messageStore.put(msg);
                                msgList.add(msg);
                            }
                        }
                    } else {
                        int i;
                        if (t <= (long) DemoTester.BOUND_2) {
                            this.messageStore.put(DemoTester
                                    .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA, t, (byte[])null), ByteBuffer.allocate(34)));
                            if ((t & 7L) == 0L) {
                                for(i = 0; i < DemoTester.REPEAT_2_MAX; ++i) {
                                    this.messageStore.put(DemoTester
                                            .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA + (long)i + 1L, t, (byte[])null), ByteBuffer.allocate(34)));
                                }

                                repeatNum = DemoTester.REPEAT_2_MAX;
                            }
                        } else if (t <= (long) DemoTester.BOUND_3) {
                            this.messageStore.put(DemoTester
                                    .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA, t, (byte[])null), ByteBuffer.allocate(34)));
                            if (t % (long) DemoTester.REPEAT_3_MASK == 0L) {
                                for(i = 0; i < DemoTester.REPEAT_3_MAX; ++i) {
                                    this.messageStore.put(DemoTester
                                            .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA + (long)i + 1L, t, (byte[])null), ByteBuffer.allocate(34)));
                                }

                                repeatNum = DemoTester.REPEAT_3_MAX;
                            }
                        } else {
                            this.messageStore.put(DemoTester
                                    .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA, t, (byte[])null), ByteBuffer.allocate(34)));
                            if ((t & 7L) == 0L) {
                                for(i = 0; i < DemoTester.REPEAT_2_MAX; ++i) {
                                    this.messageStore.put(DemoTester
                                            .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA + (long)i + 1L, t, (byte[])null), ByteBuffer.allocate(34)));
                                }

                                repeatNum = DemoTester.REPEAT_2_MAX;
                            } else if (t % (long) DemoTester.REPEAT_3_MASK == 0L) {
                                for(i = 0; i < DemoTester.REPEAT_3_MAX; ++i) {
                                    this.messageStore.put(DemoTester
                                            .generateMessageBody(new Message(t + (long) DemoTester.A_DELTA + (long)i + 1L, t, (byte[])null), ByteBuffer.allocate(34)));
                                }

                                repeatNum = DemoTester.REPEAT_3_MAX;
                            }
                        }
                    }

                    this.sendCounter.getAndAdd((long)(repeatNum + 1));
                } catch (Throwable var9) {
                    var9.printStackTrace(System.out);
                    DemoTester.error(var9.getMessage());
                    System.exit(-1);
                }
            }

        }
    }
}

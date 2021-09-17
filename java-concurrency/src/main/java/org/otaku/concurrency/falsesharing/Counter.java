package org.otaku.concurrency.falsesharing;

import org.openjdk.jol.info.ClassLayout;

/**
 *
 * <pre>
 *       Java对象的内存布局：
 *
 *       8个字节的mark，存放 锁，identityHashCode，gc 信息
 *       4个字节的klass，存放类型信息
 *       4个字节的padding（如果没有其他字段的情况下）
 *
 *       jvm为了内存对齐，会自动进行内存填充。默认以8个字节为单位，补充padding直到对象所占空间为8的整数倍为止。
 *
 *       对齐的原因：适应底层的CPU Cache机制，防止false sharing。
 * </pre>
 *
 *
 */
public class Counter {
    private long counter;
    //占位避免false sharing
    private long placeHolder;
    private final Object monitor1 = new Object();
    private final Object monitor2 = new Object();

    public void increase() {
        synchronized (monitor1) {
            counter++;
        }
    }

    public void increasePlaceHolder() {
        synchronized (monitor2) {
            placeHolder++;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ClassLayout.parseInstance(new Counter()).toPrintable());
        //测试false sharing
        testFalseSharing();
        //避免false sharing的性能提升
        avoidFalseSharing();
    }

    private static void testFalseSharing() throws Exception {
        int loopCount = 1000000000;
        Counter counter = new Counter();
        Thread t1 = new Thread(() -> {
            long start = System.currentTimeMillis();
            for (int i = 0; i < loopCount; i++) {
                counter.increase();
            }
            System.out.println("increase time cost: " + (System.currentTimeMillis() - start));
        });
        Thread t2 = new Thread(() -> {
            long start = System.currentTimeMillis();
            for (int i = 0; i < loopCount; i++) {
                counter.increasePlaceHolder();
            }
            System.out.println("increase time cost: " + (System.currentTimeMillis() - start));
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private static void avoidFalseSharing() throws Exception {
        int loopCount = 1000000000;
        Counter counter = new Counter();
        Counter counter2 = new Counter();
        Thread t1 = new Thread(() -> {
            long start = System.currentTimeMillis();
            for (int i = 0; i < loopCount; i++) {
                counter.increase();
            }
            System.out.println("increase time cost: " + (System.currentTimeMillis() - start));
        });
        Thread t2 = new Thread(() -> {
            long start = System.currentTimeMillis();
            for (int i = 0; i < loopCount; i++) {
                counter2.increase();
            }
            System.out.println("increase time cost: " + (System.currentTimeMillis() - start));
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

}

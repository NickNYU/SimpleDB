package simpledb.core;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public class ResourceLocker {
    private volatile boolean resourceAvailable = false;

    public synchronized void acquireResource() {
        while (resourceAvailable) {
            try {
                wait(); // 资源已经被其他线程占用，当前线程等待
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        resourceAvailable = true;
    }

    public synchronized void releaseResource() {
        resourceAvailable = false;
        notify(); // 通知等待资源的线程
    }
}

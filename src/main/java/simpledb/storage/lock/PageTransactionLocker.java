package simpledb.storage.lock;

import com.google.common.collect.Sets;
import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/14
 */
public class PageTransactionLocker implements Locker {

    private final PageId pageId;

    private final Set<TransactionId> shared = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> exclusive = new AtomicReference<>();

    private final AtomicBoolean lock = new AtomicBoolean(false);

    public PageTransactionLocker(PageId pageId) {
        this.pageId = pageId;
    }

    @Override
    public boolean tryLock(LockContext context, int time, TimeUnit timeUnit) throws InterruptedException {
        // if we have exclusive lock, check if transaction Id matches, otherwise going for more lock waiting logic
        if (context.getTransactionId().equals(exclusive.get())) {
            return true;
        }
        long timeoutNano = timeUnit.toNanos(time) + System.nanoTime();
        switch (context.getPermission()) {
            case READ_ONLY:
                // lock has been acquired, but not same with transaction Id given by context (above logic)
                for (;;) {
                    if (timeoutNano - System.nanoTime() <= 0) {
                        System.err.println("timed out: " + context);
                        return false;
                    }
                    if (lock.get()) {
                        Thread.sleep(1);
                        continue;
                    }
                    // take care for read/write concurrency
                    synchronized (this) {
                        shared.add(context.getTransactionId());
                        return true;
                    }
                }
            case READ_WRITE:
                if (shared.isEmpty() && lock.compareAndSet(false, true)) {
                    synchronized (this) {
                        if (isLockUpgradable(context.getTransactionId())) {
                            exclusive.set(context.getTransactionId());
                            return true;
                        } else {
                            lock.set(false);
                        }
                    }
                }
                for (;;) {
                    if (timeoutNano - System.nanoTime() <= 0) {
                        System.err.println("timed out: " + context);
                        return false;
                    }
                    if (lock.get() || !isLockUpgradable(context.getTransactionId())) {
                        Thread.sleep(1);
                        continue;
                    }
                    // take care for read/write concurrency
                    synchronized (this) {
                        if (lock.compareAndSet(false, true)) {
                            exclusive.set(context.getTransactionId());
                            shared.remove(context.getTransactionId());
                            return true;
                        }
                    }
                }
        }
        return false;
    }

    private boolean isLockUpgradable(TransactionId transactionId) {
        return shared.isEmpty() || (shared.size() == 1 && shared.contains(transactionId));
    }

    @Override
    public boolean hasHolder(TransactionId transactionId, Permissions permissions) {
        switch (permissions) {
            case READ_ONLY:
                return shared.contains(transactionId) || transactionId.equals(exclusive.get());
            case READ_WRITE:
                return transactionId.equals(exclusive.get());
        }
        return false;
    }

    @Override
    public void release(TransactionId transactionId) {
        shared.remove(transactionId);
        if (transactionId.equals(exclusive.get())) {
            exclusive.set(null);
            lock.set(false);
        }
    }
}

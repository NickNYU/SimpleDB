package simpledb.storage.lock;

import com.google.common.collect.Sets;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/14
 */
public class TransactionLocker implements Locker {

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final Set<TransactionId> shared = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> exclusive = new AtomicReference<>();

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
    public void hold(TransactionId transactionId, Permissions permissions) {
        switch (permissions) {
            case READ_ONLY:
                shared.add(transactionId);
            case READ_WRITE:
                exclusive.set(transactionId);
        }
    }

    @Override
    public void release(TransactionId transactionId) {
        if (transactionId.equals(exclusive.get())) {
            exclusive.set(null);
        } else {
            shared.remove(transactionId);
        }
    }

    @Override
    public boolean tryLock(int time, TimeUnit timeUnit, Permissions permissions) throws InterruptedException {
        switch (permissions) {
            case READ_ONLY:
                return lock.readLock().tryLock(time, timeUnit);
            case READ_WRITE:
                return lock.writeLock().tryLock(time, timeUnit);
        }
        return false;
    }
}

package simpledb.storage.lock;

import com.google.common.collect.Maps;
import simpledb.common.Permissions;
import simpledb.core.exception.CycleDetectedException;
import simpledb.storage.PageId;
import simpledb.storage.transaction.DefaultTransactionManager;
import simpledb.storage.transaction.TransactionManager;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentMap;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/13
 */
public class DefaultLockManager implements LockManager {

    private final TransactionManager transactionManager;
    private final ConcurrentMap<PageId, LockManager> lockers = Maps.newConcurrentMap();

    public DefaultLockManager() {
        this.transactionManager = new DefaultTransactionManager(this);
    }

    @Override
    public Locker getLock(TransactionId transactionId, PageId pageId) {
        lockers.putIfAbsent(pageId, new PageLockManager(pageId));
        return lockers.get(pageId).getLock(transactionId, pageId);
    }

    @Override
    public void record(TransactionId transactionId, PageId pageId, Permissions permissions) throws CycleDetectedException {
        transactionManager.addPage(pageId, transactionId);
        lockers.get(pageId).record(transactionId, pageId, permissions);
    }

    @Override
    public void release(TransactionId transactionId, PageId pageId) {
        lockers.get(pageId).release(transactionId, pageId);
    }

    @Override
    public void releaseAll(TransactionId transactionId) {
        transactionManager.release(transactionId);
    }

    @Override
    public boolean hasLock(TransactionId transactionId, PageId pageId) {
        if (!lockers.containsKey(pageId)) {
            return false;
        }
        return lockers.get(pageId).hasLock(transactionId, pageId);
    }
}

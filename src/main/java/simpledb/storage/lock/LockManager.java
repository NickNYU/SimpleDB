package simpledb.storage.lock;

import simpledb.common.Permissions;
import simpledb.core.exception.CycleDetectedException;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/13
 */
public interface LockManager {

    /**
     * Returns the lock used for writing.
     *
     * @return the lock used for writing
     */
    Locker getLock(TransactionId transactionId, PageId pageId);

    void record(TransactionId transactionId, PageId pageId, Permissions permissions) throws CycleDetectedException;

    /**
     * Returns the lock used for writing.
     *
     */
    void release(TransactionId transactionId, PageId pageId);

    void releaseAll(TransactionId transactionId);

    boolean hasLock(TransactionId transactionId, PageId pageId);
}

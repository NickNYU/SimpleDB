package simpledb.storage.lock;

import com.google.common.collect.Sets;
import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.Set;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/13
 */
public class PageLockManager implements LockManager {

    private final PageId pageId;

    private final Set<TransactionId> transactionIds;

    private final Locker locker;

    public PageLockManager(PageId pageId) {
        this.pageId = pageId;
        this.transactionIds = Sets.newConcurrentHashSet();
        this.locker = new PageTransactionLocker(pageId);
    }

    @Override
    public Locker getLock(TransactionId transactionId, PageId pageId) {
        if (!this.pageId.equals(pageId)) {
            throw new IllegalArgumentException("pageId not match, expected: " + this.pageId + ", actual: " + pageId);
        }
        return locker;
    }

    @Override
    public void record(TransactionId transactionId, PageId pageId, Permissions permissions) {
        transactionIds.add(transactionId);
    }

    @Override
    public void release(TransactionId transactionId, PageId pageId) {
        if (!this.pageId.equals(pageId)) {
            throw new IllegalArgumentException("pageId not match, expected: " + this.pageId + ", actual: " + pageId);
        }
        transactionIds.remove(transactionId);
        locker.release(transactionId);
    }

    @Override
    public void releaseAll(TransactionId transactionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasLock(TransactionId transactionId, PageId pageId) {
        if (!this.pageId.equals(pageId)) {
            return false;
        }
        return transactionIds.contains(transactionId);
    }


}

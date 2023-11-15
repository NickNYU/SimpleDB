package simpledb.storage.transaction;

import com.google.common.collect.Sets;
import simpledb.storage.PageId;
import simpledb.storage.lock.LockManager;
import simpledb.transaction.TransactionId;

import java.util.Set;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/14
 */
public class SimpleTransactionManager implements TransactionManager {

    private final TransactionId transactionId;

    private final LockManager lockManager;

    private final Set<PageId> pages = Sets.newHashSet();

    public SimpleTransactionManager(TransactionId transactionId, LockManager lockManager) {
        this.transactionId = transactionId;
        this.lockManager = lockManager;
    }

    @Override
    public void addPage(PageId pageId, TransactionId transactionId) {
        if (!this.transactionId.equals(transactionId)) {
            throw new IllegalArgumentException("transactionId not match, expected: " + this.transactionId
                    + ", actual: " + transactionId);
        }
        pages.add(pageId);
    }

    @Override
    public void release(TransactionId transactionId) {
        pages.forEach(pageId -> lockManager.release(transactionId, pageId));
    }
}

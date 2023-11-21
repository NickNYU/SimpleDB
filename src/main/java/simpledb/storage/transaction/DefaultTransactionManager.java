package simpledb.storage.transaction;

import com.google.common.collect.Maps;
import simpledb.core.exception.CycleDetectedException;
import simpledb.storage.PageId;
import simpledb.storage.lock.LockManager;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentMap;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/14
 */
public class DefaultTransactionManager implements TransactionManager {

    private final LockManager lockManager;

    private final ConcurrentMap<TransactionId, SimpleTransactionManager> transactions = Maps.newConcurrentMap();

    public DefaultTransactionManager(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public void addPage(PageId pageId, TransactionId transactionId) throws CycleDetectedException {
        transactions.putIfAbsent(transactionId, new SimpleTransactionManager(transactionId, lockManager));
        transactions.get(transactionId).addPage(pageId, transactionId);
    }

    @Override
    public void release(TransactionId transactionId) {
        SimpleTransactionManager transactionManager = transactions.remove(transactionId);
        if (transactionManager != null) {
            transactionManager.release(transactionId);
        }
    }
}

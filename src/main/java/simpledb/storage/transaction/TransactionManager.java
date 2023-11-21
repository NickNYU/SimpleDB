package simpledb.storage.transaction;

import simpledb.core.exception.CycleDetectedException;
import simpledb.storage.PageId;
import simpledb.storage.lock.Locker;
import simpledb.transaction.TransactionId;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/13
 */
public interface TransactionManager {

    void addPage(PageId pageId, TransactionId transactionId) throws CycleDetectedException;

    void release(TransactionId transactionId);

}

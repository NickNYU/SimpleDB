package simpledb.storage.lock;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.concurrent.TimeUnit;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/13
 */
public interface Locker {

    boolean hasHolder(TransactionId transactionId, Permissions permissions);

    void hold(TransactionId transactionId, Permissions permissions);

    void release(TransactionId transactionId);

    boolean tryLock(int time, TimeUnit timeUnit, Permissions permissions) throws InterruptedException;
}

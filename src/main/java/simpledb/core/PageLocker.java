package simpledb.core;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.transaction.TransactionId;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public interface PageLocker {

    Page getPage(TransactionId transactionId, Permissions permission);

    void release();
}

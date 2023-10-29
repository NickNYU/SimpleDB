package simpledb.storage.page;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public interface PageManager {
    Page getOrCreate(PageId pageId, TransactionId transactionId, Permissions permissions);
}

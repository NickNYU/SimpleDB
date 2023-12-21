package simpledb.storage.page;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public interface PageManager {
    Page getOrCreate(PageId pageId, TransactionId transactionId, Permissions permissions);

    Page get(PageId pageId);

    void add(Page page);

    void remove(PageId pageId);

    void refresh(Page page);

    void traverse(Traverser traverser);

    void evict(EvictFunction evictFunction);

    public interface Traverser {
        void action(Page page) throws IOException;
    }

    public interface EvictFunction {
        void action(Page page);
    }
}

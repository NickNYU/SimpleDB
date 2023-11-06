package simpledb.storage.page;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public class DefaultPageManager implements PageManager {

    private final ConcurrentMap<PageId, Page> pages = new ConcurrentHashMap<>();

    @Override
    public Page getOrCreate(PageId pageId, TransactionId transactionId, Permissions permissions) {
        if (!pages.containsKey(pageId)) {
            synchronized (this) {
                if (!pages.containsKey(pageId)) {
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                    Page page = dbFile.readPage(pageId);
                    pages.put(pageId, page);
                }
            }
        }
        return pages.get(pageId);
    }

    @Override
    public Page get(PageId pageId) {
        return pages.get(pageId);
    }

    @Override
    public void add(Page page) {
        pages.put(page.getId(), page);
    }

    @Override
    public void remove(PageId pageId) {
        pages.remove(pageId);
    }

    @Override
    public void refresh(Page page) {
        //todo: add LRU refresh logic
    }

    @Override
    public void traverse(Traverser traverser) {
        pages.forEach((pageId, page) -> {traverser.action(page);});
    }

    @Override
    public void evict(EvictFunction evictFunction) {

    }
}

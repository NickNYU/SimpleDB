package simpledb.storage.page;

import com.google.common.collect.Maps;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public class DefaultPageManager implements PageManager {

    private final ConcurrentMap<PageId, ListNode> pages = new ConcurrentHashMap<>();

    private ListNode head = new ListNode(null);

    private ListNode tail = new ListNode(null);

    private final int capacity;

    public DefaultPageManager(int capacity) {
        this.capacity = capacity;
        head.next = tail;
        tail.prev = head;
    }

    @Override
    public Page getOrCreate(PageId pageId, TransactionId transactionId, Permissions permissions) {
        if (!pages.containsKey(pageId)) {
            synchronized (this) {
                if (!pages.containsKey(pageId)) {
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                    Page page = dbFile.readPage(pageId);
                    internalAdd(page);
                    if (pages.size() > this.capacity) {
                        removeLast();
                    }
                }
            }
        }
        return get(pageId);
    }

    @Override
    public Page get(PageId pageId) {
        ListNode node = pages.get(pageId);
        if (node == null) {
            return null;
        }
        popNode(node);
        return node.page;
    }
    private void internalAdd(Page page) {
        pages.put(page.getId(), new ListNode(page));
        popNode(pages.get(page.getId()));
    }

    @Override
    public void add(Page page) {
        if (!pages.containsKey(page.getId())) {
            internalAdd(page);
            if (pages.size() > this.capacity) {
                removeLast();
            }
        } else {
            popNode(pages.get(page.getId()));
        }
    }

    @Override
    public void remove(PageId pageId) {
        ListNode node = pages.remove(pageId);
        removeNode(node);
    }

    @Override
    public void refresh(Page page) {
        popNode(pages.get(page.getId()));
    }

    @Override
    public void traverse(Traverser traverser) {
        pages.forEach((pageId, node) -> {traverser.action(node.page);});
    }

    @Override
    public void evict(EvictFunction evictFunction) {
        Map<PageId, ListNode> iterator = Maps.newHashMap(this.pages);
        iterator.forEach((pageId, node) -> {
            if (node.page.isDirty() != null) {
                remove(pageId);
                evictFunction.action(node.page);
            }
        });
    }

    private void popNode(ListNode node) {
        removeNode(node);

        ListNode prevFirst = head.next;
        head.next = node;
        node.prev = head;

        node.next = prevFirst;
        prevFirst.prev = node;
    }

    private void removeNode(ListNode node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
    }

    /**
     * if the page is dirty (holds a transactionId), we cannot remove it as it's not able to flush it yet (transaction not finished)
     */
    private void removeLast() {
        ListNode last = tail.prev;
        while (last != head && last.page.isDirty() != null) {
            last = last.prev;
        }
        if (last == null) {
            return;
        }
        removeNode(last);
        pages.remove(last.page.getId());
    }

    private static final class ListNode {
        private final Page page;

        private ListNode prev;

        private ListNode next;

        public ListNode(Page page) {
            this.page = page;
        }
    }
}

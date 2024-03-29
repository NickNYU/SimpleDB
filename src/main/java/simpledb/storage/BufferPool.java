package simpledb.storage;

import com.google.common.collect.Sets;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.core.exception.CycleDetectedException;
import simpledb.storage.lock.DefaultLockContext;
import simpledb.storage.lock.DefaultLockManager;
import simpledb.storage.lock.LockManager;
import simpledb.storage.lock.Locker;
import simpledb.storage.page.DefaultPageManager;
import simpledb.storage.page.PageManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final PageManager pageManager;

    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pageManager = new DefaultPageManager(numPages);
        lockManager = new DefaultLockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException,
                                                                        DbException {
        // some code goes here
        Locker locker = lockManager.getLock(tid, pid);
        try {
            if (!locker.tryLock(DefaultLockContext.builder()
                            .permissions(perm)
                            .transactionId(tid)
                            .pageId(pid).build(),
                    100, TimeUnit.MILLISECONDS)) {
                throw new TransactionAbortedException("tid: " + tid + ", pid: " + pid + ", perm: " + perm);
            }
            lockManager.record(tid, pid, perm);
            return pageManager.getOrCreate(pid, tid, perm);
        } catch (InterruptedException | CycleDetectedException e) {
            throw new TransactionAbortedException(e);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseAll(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            flushLogs(tid);
            flushPages(tid);
        } else {
            discardPages(tid);
        }
        lockManager.releaseAll(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException,
                                                                    TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.insertTuple(tid, t);
        for (Page page : pages) {
            pageManager.add(page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        pageManager.traverse(new PageManager.Traverser() {
            @Override
            public void action(Page page) {
                if (page.isDirty() != null) {
                    try {
                        flushPage(page);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageManager.remove(pid);
    }

    public synchronized void discardPages(TransactionId transactionId) {
        Set<Page> toBeRemoved = Sets.newHashSet();
        pageManager.traverse(new PageManager.Traverser() {
            @Override
            public void action(Page page) {
                if (transactionId.equals(page.isDirty())) {
                    toBeRemoved.add(page);
                }
            }
        });
        toBeRemoved.forEach(this::discardPage);
    }

    private void discardPage(Page page) {
//        try {
            pageManager.remove(page.getId());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param page an ID indicating the page to flush
     */
    private synchronized void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            // for lab6, write update record first
            if (page.isDirty() != null) {
                final LogFile logFile = Database.getLogFile();
                logFile.logWrite(page.isDirty(), page.getBeforeImage(), page);
                logFile.force();
            }

            // Write page
            final DbFile tableFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            tableFile.writePage(page);
            page.markDirty(false, null);
            page.setBeforeImage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        pageManager.traverse(new PageManager.Traverser() {
            @Override
            public void action(Page page) throws IOException {
                if (tid.equals(page.isDirty())) {
                    flushPage(page);
                }
            }
        });
    }

    private synchronized void flushLogs(TransactionId tid) {
        pageManager.traverse(new PageManager.Traverser() {
            @Override
            public void action(Page page) throws IOException {
                if (tid.equals(page.isDirty())) {
                    page.setBeforeImage();
                }
            }
        });
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        AtomicBoolean isAllDirty = new AtomicBoolean(true);
        AtomicBoolean stop = new AtomicBoolean(false);
        pageManager.evict(new PageManager.EvictFunction() {
            @Override
            public void action(Page page) {
                if (stop.get()) {
                    return;
                }
                if (page.isDirty() == null) {
                    discardPage(page.getId());
                    stop.set(true);
                    isAllDirty.set(false);
                }
            }
        });
        if (isAllDirty.get()) {
            throw new DbException("All pages are dirty in buffer pool");
        }
    }

    private void writePage(Page page) {
        try {
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

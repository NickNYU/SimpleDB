package simpledb.storage;

import com.google.common.collect.Lists;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.core.file.ByteArrayFileReader;
import simpledb.core.file.RandomAccessFileWriter;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc tupleDesc;

    private final FileMeta fileMeta;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.fileMeta = new FileMeta(f);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return fileMeta.fd;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            int pageNum = pid.getPageNumber();
            int begin = BufferPool.getPageSize() * pageNum;
            int end = BufferPool.getPageSize() * (pageNum + 1);
            HeapPageId pageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(pageId, new ByteArrayFileReader().getContent(this.file, begin, end));
        } catch (Throwable throwable) {
            throw new IllegalArgumentException(throwable);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        new RandomAccessFileWriter().writePage(file, getPageStartPosition(page), page);
    }

    private int getPageStartPosition(Page page) {
        return page.getId().getPageNumber() * BufferPool.getPageSize();
    }

    /**
     * Returns the number of pages in this HeapFile.
     * _tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException,
                                                             TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        BufferPool bufferPool = Database.getBufferPool();
        for (int pageNum = 0; pageNum < numPages(); pageNum++) {
            HeapPage heapPage = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), pageNum), Permissions.READ_WRITE);
            if (heapPage == null || heapPage.getNumEmptySlots() == 0) {
                continue;
            }
            heapPage.insertTuple(t);
            heapPage.markDirty(true, tid);
            return Lists.newArrayList(heapPage);
        }
        // go into insert page logic
        final HeapPageId heapPageId = new HeapPageId(getId(), this.numPages());
        HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        writePage(newPage);
        // Through buffer pool to get newPage
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        heapPage.markDirty(true, tid);
        return Lists.newArrayList(heapPage);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
//        page.markDirty(true, tid);
        page.deleteTuple(t);
        return Lists.newArrayList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    private static final class FileMeta {
        private int fd;

        private FileMeta(File file) {
            this.fd = file.getAbsoluteFile().hashCode();
        }
    }

    private static final class HeapFileIterator extends AbstractDbFileIterator {

        private final TransactionId transactionId;

        private final HeapFile heapFile;

        private int pageNum = 0;

        private Iterator<Tuple> tupleIterator;

        private final AtomicBoolean switcher = new AtomicBoolean(false);

        private final AtomicBoolean closeGate = new AtomicBoolean(false);

        public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (closeGate.get()) {
                throw new NoSuchElementException("iterator not open yet");
            }
            if (!switcher.get() || pageNum > heapFile.numPages()) {
                return null;
            }
            if (tupleIterator != null && tupleIterator.hasNext()) {
                return tupleIterator.next();
            }
            while (pageNum < heapFile.numPages() && (tupleIterator == null || !tupleIterator.hasNext())) {
                HeapPage page = (HeapPage) Database.getBufferPool()
                        .getPage(transactionId,
                                new HeapPageId(heapFile.getId(), pageNum),
                                Permissions.READ_ONLY);
                tupleIterator = page.iterator();
                pageNum ++;
            }
            return tupleIterator.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            switcher.compareAndSet(false, true);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageNum = 0;
            tupleIterator = null;
        }

        @Override
        public void close() {
            closeGate.compareAndSet(false, true);
            super.close();
        }
    }

}

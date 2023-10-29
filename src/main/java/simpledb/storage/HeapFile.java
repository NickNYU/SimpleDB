package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
//        return Database.getBufferPool().getPage(null, pid, Permissions.READ_ONLY);
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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

        private AtomicInteger indexer = new AtomicInteger(0);

        public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {

            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            int tupleSize = heapFile.getTupleDesc().getSize();
            int pageSize = BufferPool.getPageSize();
            long heapFileSize = heapFile.file.length();
            long numOfPages = heapFileSize / pageSize;
//            PageId pageId = new HeapPageId();
//            Database.getBufferPool().getPage(transactionId, 1, Permissions.READ_ONLY);
//            RandomAccessFile randomAccessFile = new RandomAccessFile(heapFile.getFile());

        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

        }
    }

}

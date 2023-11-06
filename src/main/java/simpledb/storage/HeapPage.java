package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
//TODO: add freelist, to hold deleted tuples
// 1. when insert, use free tuples first, and release free list
// 2. when delete, send delete tuple to free list, and free used tuple
public class HeapPage implements Page {

    final HeapPageId   pid;
    final TupleDesc tupleDesc;
    final byte[]       header;
    final Tuple[]      tuples;
    final int          numSlots;

    byte[]             oldData;
    private final Byte oldDataLock = (byte) 0;

    private TransactionId transactionId;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
        `
        _tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))
        `
    */
    private int getNumTuples() {
        // some code goes here
        return BufferPool.getPageSize() * 8 / (tupleDesc.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * headerBytes = ceiling(tupsPerPage/8)
     */
    private int getHeaderSize() {
        return (getNumTuples() + 7) / 8;

    }

    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(tupleDesc);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                Field f = tupleDesc.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        final RecordId recordId = t.getRecordId();
        final HeapPageId pageId = (HeapPageId) recordId.getPageId();
        final int tn = recordId.getTupleNumber();
        if (!pageId.equals(this.pid)) {
            throw new DbException("Page id not match");
        }
        if (!isSlotUsed(tn)) {
            throw new DbException("Slot is not used");
        }
        markSlotUsed(tn, false);
        this.tuples[tn] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!this.tupleDesc.equals(t.getTupleDesc())) {
            throw new DbException("tuple desc not matched: " + t.getTupleDesc());
        }
        for (int tupleNumber = 0; tupleNumber < getNumTuples(); tupleNumber++) {
            if (isSlotUsed(tupleNumber)) {
                continue;
            }
            markSlotUsed(tupleNumber, true);
            t.setRecordId(new RecordId(this.pid, tupleNumber));
            this.tuples[tupleNumber] = t;
            return;
        }
        throw new DbException("The page is full");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        if (dirty) {
            this.transactionId = tid;
        } else {
            this.transactionId = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return this.transactionId;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int counter = 0;
        for (int slot = 0; slot < numSlots; slot ++) {
            if (!isSlotUsed(slot)) {
                counter ++;
            }
        }
        return counter;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        if (i >= numSlots) {
            return false;
        }
        int index = i / Byte.SIZE;
        int pos = i % Byte.SIZE;
        return ((header[index]) & (1 << pos)) > 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int tupleNum, boolean value) {
        // some code goes here
        // not necessary for lab1
        int index = tupleNum / Byte.SIZE;
        int pos = tupleNum % Byte.SIZE;
        if (value) {
            header[index] |= (1 << pos);
        } else {
            byte sign = (byte)(1 << pos);
            sign = (byte) ~sign;
            header[index] = (byte) (sign & header[index]);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageIterator(this);
    }

    private static final class HeapPageIterator implements Iterator<Tuple> {

        private final HeapPage heapPage;

        private int indexer = 0;

        public HeapPageIterator(HeapPage heapPage) {
            this.heapPage = heapPage;
        }

        @Override
        public boolean hasNext() {
            while (indexer < heapPage.numSlots && !heapPage.isSlotUsed(indexer)) {
                indexer ++;
            }
            return indexer < heapPage.numSlots;
        }

        @Override
        public Tuple next() {
            if (indexer >= heapPage.numSlots) {
                return null;
            }
            Tuple tuple = heapPage.tuples[indexer];
            indexer ++;
            return tuple;
        }
    }

}

package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.FieldType;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TupleDesc tupleDesc;

    private final int tableId;

    private OpIterator child;

    private final TransactionId transactionId;

    private boolean done = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // some code goes here
        try {
            this.child = child;
            this.transactionId = t;
            this.tableId = tableId;
            this.tupleDesc = new TupleDesc(new FieldType[]{FieldType.INT_TYPE});
        } catch (Throwable throwable) {
            throw new DbException(throwable.getMessage());
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
        this.done = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (done) {
            return null;
        }
        int counter = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(this.transactionId, tableId, tuple);
            } catch (IOException e) {
                throw new TransactionAbortedException();
            }
            counter++;
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(counter));
        done = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}

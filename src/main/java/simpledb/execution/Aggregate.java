package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.FieldType;
import simpledb.execution.aggregator.DefaultAggregator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private final int aggregateFieldIndex;

    private final int groupByFieldIndex;

    private final Aggregator.Op operator;

    private final FieldType fieldType;

    private TupleIterator iterator;

    private TupleDesc td;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link Aggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggregateFieldIndex = afield;
        this.groupByFieldIndex = gfield;
        this.operator = aop;
        TupleDesc originTd = this.child.getTupleDesc();
        this.fieldType = (gfield == Aggregator.NO_GROUPING ? null : originTd.getFieldType(gfield));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.child.getTupleDesc().getFieldName(this.groupByFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.child.getTupleDesc().getFieldName(this.aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();

        // Build aggregator
        Aggregator aggregator = new DefaultAggregator(this.groupByFieldIndex, this.fieldType, this.aggregateFieldIndex, this.operator);

        // Merge tuples into group
        while (this.child.hasNext()) {
            final Tuple tuple = this.child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        this.iterator = (TupleIterator) aggregator.iterator();
        this.iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (this.td != null) {
            return this.td;
        }
        this.td = createTupleDesc();
        return this.td;
    }

    private TupleDesc createTupleDesc() {
        FieldType[] types;
        String[] names;
        if (this.groupByFieldIndex == NO_GROUPING) {
            types = new FieldType[]{FieldType.INT_TYPE};
            names = new String[]{this.aggregateFieldName()};
        } else {
            types = new FieldType[]{this.fieldType, FieldType.INT_TYPE};
            names = new String[]{this.groupFieldName(), this.aggregateFieldName()};
        }
        return new TupleDesc(types, names);
    }

    public void close() {
        // some code goes here
        this.child.close();
        this.iterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}

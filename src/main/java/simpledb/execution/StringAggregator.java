package simpledb.execution;

import simpledb.common.FieldType;
import simpledb.execution.aggregator.AbstractAggregator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, FieldType gbfieldtype, int afield, Op what) {
        // some code goes here
        super(gbfield, gbfieldtype, afield, what);
    }

    @Override
    protected void generateTupleDescIfNeeded(Tuple tuple) {
        if (tupleDesc != null) {
            return;
        }
        TupleDesc origin = tuple.getTupleDesc();
        if (isNonGroupingBy()) {
            FieldType[] types = new FieldType[] { FieldType.INT_TYPE };
            String[] names = new String[] { "" };
            this.tupleDesc = new TupleDesc(types, names);
        } else {
            FieldType[] types = new FieldType[] { factory.getFieldType(), FieldType.INT_TYPE };
            String[] names = new String[] { origin.getFieldName(factory.getGroupByFieldIndex()), origin.getFieldName(factory.getAggregatedFieldIndex()) };
            this.tupleDesc = new TupleDesc(types, names);
        }
    }

}

package simpledb.execution.aggregator;

import simpledb.common.FieldType;
import simpledb.execution.Aggregator;
import simpledb.execution.OpIterator;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public abstract class AbstractAggregator implements Aggregator {

    protected TupleDesc tupleDesc;

    protected final GroupAggregatorFactory factory;

    private final Map<Field, GroupAggregator> groupByFieldIndexer = new HashMap<>();

    public AbstractAggregator(int gbfield, FieldType gbfieldtype, int afield, Op what) {
        // some code goes here
        this.factory = new GroupAggregatorFactory(gbfieldtype, gbfield, afield, what);
    }

    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        generateTupleDescIfNeeded(tup);
        getOrCreate(tup).aggregate(tup);
    }

    @Override
    public OpIterator iterator() {
        final List<Tuple> tuples;
        if (!isNonGroupingBy()) {
            tuples = new ArrayList<>(groupByFieldIndexer.size());
            this.groupByFieldIndexer.forEach((key, aggregator) -> {
                final Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, key);
                tuple.setField(1, getAggregatedField(aggregator));
                tuples.add(tuple);
            });
        } else {
            tuples = new ArrayList<>(1);
            final Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, getAggregatedField(this.groupByFieldIndexer.get(factory.DEFAULT_FIELD)));
            tuples.add(tuple);
        }
        return new TupleIterator(this.tupleDesc, tuples);
    }

    private Field getAggregatedField(GroupAggregator groupAggregator) {
        return new IntField(groupAggregator.getResult());
    }

    protected abstract void generateTupleDescIfNeeded(Tuple tup);

    private GroupAggregator getOrCreate(Tuple tuple) {
        GroupAggregator aggregator = groupByFieldIndexer.get(factory.getGroupKey(tuple));
        if (aggregator == null) {
            aggregator = factory.create();
            groupByFieldIndexer.put(factory.getGroupKey(tuple), aggregator);
        }
        return aggregator;
    }

    protected boolean isNonGroupingBy() {
        return factory.isNonGroupingBy();
    }

    protected static final class GroupAggregatorFactory {

        private static final Field DEFAULT_INT_FIELD = new IntField(0);

        private static final Field DEFAULT_STRING_FIELD = new StringField("", 1);

        private final Field DEFAULT_FIELD;
        private final FieldType fieldType;

        private final int groupByFieldIndex;

        private final int aggregatedFieldIndex;

        private final Op op;

        public GroupAggregatorFactory(FieldType fieldType, int groupByFieldIndex, int aggregatedFieldIndex, Op op) {
            this.fieldType = fieldType;
            this.groupByFieldIndex = groupByFieldIndex;
            this.aggregatedFieldIndex = aggregatedFieldIndex;
            this.op = op;
            this.DEFAULT_FIELD = fieldType == FieldType.INT_TYPE ? DEFAULT_INT_FIELD : DEFAULT_STRING_FIELD;
        }

        private Field getGroupKey(Tuple tuple) {
            if (isNonGroupingBy()) {
                return DEFAULT_FIELD;
            }
            return tuple.getField(groupByFieldIndex);
        }

        public boolean isNonGroupingBy() {
            return groupByFieldIndex == NO_GROUPING;
        }

        public int getGroupByFieldIndex() {
            return this.groupByFieldIndex;
        }

        public int getAggregatedFieldIndex() {
            return this.aggregatedFieldIndex;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        private GroupAggregator create() {
            switch (fieldType) {
                case INT_TYPE:
                    return new IntegerGroupAggregator(groupByFieldIndex, aggregatedFieldIndex, op);
                case STRING_TYPE:
                    return new StringGroupAggregator(groupByFieldIndex, aggregatedFieldIndex, op);
            }
            throw new UnsupportedOperationException("FieldType: " + fieldType);
        }
    }
}

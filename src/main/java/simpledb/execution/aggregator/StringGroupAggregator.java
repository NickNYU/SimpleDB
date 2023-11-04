package simpledb.execution.aggregator;

import simpledb.execution.Aggregator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public class StringGroupAggregator extends AbstractGroupAggregator {

    public StringGroupAggregator(int groupByField, int aggregatorField, Aggregator.Op op) {
        super(groupByField, aggregatorField, op);
    }

    @Override
    protected int calculateMax(Calculator calculator, Field aggregator) {
        throw new IllegalArgumentException("String field not support max op");
    }

    @Override
    protected int calculateSum(Calculator calculator, Field aggregator) {
        return calculator.getSum() + ((IntField) aggregator).getValue();
    }

    @Override
    protected int calculateMin(Calculator calculator, Field aggregator) {
        throw new IllegalArgumentException("String field not support max op");
    }
}

package simpledb.execution.aggregator;

import simpledb.execution.Aggregator;
import simpledb.storage.Field;
import simpledb.storage.IntField;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public class IntegerGroupAggregator extends AbstractGroupAggregator {

    public IntegerGroupAggregator(int groupByField, int aggregatorField, Aggregator.Op op) {
        super(groupByField, aggregatorField, op);
    }

    @Override
    public int calculateMax(Calculator calculator, Field aggregator) {
        return Math.max(calculator.getMax(), ((IntField) aggregator).getValue());
    }

    @Override
    public int calculateSum(Calculator calculator, Field aggregator) {
        return calculator.getSum() + ((IntField) aggregator).getValue();
    }

    @Override
    public int calculateMin(Calculator calculator, Field aggregator) {
        return Math.min(calculator.getMin(), ((IntField) aggregator).getValue());
    }

}

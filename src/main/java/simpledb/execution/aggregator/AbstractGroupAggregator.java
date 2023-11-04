package simpledb.execution.aggregator;

import simpledb.execution.Aggregator;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public abstract class AbstractGroupAggregator implements GroupAggregator {

    protected final int groupByField;

    protected final int aggregatorField;

    protected final Aggregator.Op operator;

    protected final Calculator calculator;

    public AbstractGroupAggregator(int groupByField, int aggregatorField, Aggregator.Op op) {
        this.groupByField = groupByField;
        this.aggregatorField = aggregatorField;
        this.operator = op;
        this.calculator = new Calculator();
    }

    @Override
    public void aggregate(Tuple tuple) {
        Field aggregator = tuple.getField(aggregatorField);
        switch (operator) {
            case MIN:
                calculator.min = calculateMin(calculator, aggregator);
                break;
            case AVG:
                calculator.number = calculateSum(calculator, aggregator);
                calculator.increase();
                break;
            case SUM:
                calculator.number = calculateSum(calculator, aggregator);
                break;
            case COUNT:
                calculator.increase();
                break;
            case MAX:
                calculator.max = calculateMax(calculator, aggregator);
                break;
        }
    }

    protected abstract int calculateMax(Calculator calculator, Field aggregator);

    protected abstract int calculateSum(Calculator calculator, Field aggregator);

    protected abstract int calculateMin(Calculator calculator, Field aggregator);

    @Override
    public int getResult() {
        switch (operator) {
            case MIN:
                return calculator.getMin();
            case AVG:
                return calculator.getAvg();
            case SUM:
                return calculator.getSum();
            case COUNT:
                return calculator.getCounter();
            case MAX:
                return calculator.getMax();
        }
        return -1;
    }

    @Override
    public int getAggregateField() {
        return this.aggregatorField;
    }

    @Override
    public int getGroupByField() {
        return this.groupByField;
    }

    protected static final class Calculator {
        private int counter = 0;

        private int number = 0;

        private int max = Integer.MIN_VALUE;

        private int min = Integer.MAX_VALUE;

        public int getCounter() {
            return counter;
        }

        public int getMax() {
            return max;
        }

        public int getMin() {
            return min;
        }

        public int getAvg() {
            return number / counter;
        }

        public int getSum() {
            return number;
        }

        public void increase() {
            this.counter ++;
        }

    }

}

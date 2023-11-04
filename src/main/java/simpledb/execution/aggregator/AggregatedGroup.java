package simpledb.execution.aggregator;

import simpledb.storage.IntField;
import simpledb.storage.Tuple;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public interface AggregatedGroup {

    void aggregate(Tuple tuple);

    IntField getResult();

    int getAggregateField();

    int getGroupByField();

}

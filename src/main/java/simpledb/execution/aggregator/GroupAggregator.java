package simpledb.execution.aggregator;

import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public interface GroupAggregator {

    void aggregate(Tuple tuple);

    int getResult();

    int getAggregateField();

    int getGroupByField();

}

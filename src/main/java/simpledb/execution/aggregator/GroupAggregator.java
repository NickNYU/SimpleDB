package simpledb.execution.aggregator;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/11/4
 */
public interface GroupAggregator {

    void aggregate(Tuple tuple);

    IntField getResult();

    int getAggregateField();

    int getGroupByField();

}

package simpledb.utils;

import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;


public interface AggUtil {
    void operate(Tuple tup);
    TupleDesc getTupleDesc();
    Iterable<Tuple> getTuples();
}
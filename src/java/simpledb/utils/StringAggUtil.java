package simpledb.utils;

import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.Map;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import java.util.List;
import java.util.ArrayList;

public class StringAggUtil implements AggUtil{

    private final Map<Field, Integer> groups;
    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private static final Type afieldType = Type.INT_TYPE;
    private static final Field PRESENT = new IntField(-1);
    private final Op what;
    private List<Tuple> tuples = new ArrayList<>();
    private TupleDesc td;
    public StringAggUtil(Map<Field, Integer> groups, int gbfield, Type gbfieldType, int afield, Op what) {
        this.groups = groups;
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldType;
        this.afield = afield;
        this.what = what;
    }

    @Override
    public void operate(Tuple tup) {
        if (td == null) {
            if (gbfieldType != null) {
                td = new TupleDesc(new Type[]{gbfieldType, afieldType}, new String[]{tup.getTupleDesc().getFieldName(gbfield), tup.getTupleDesc().getFieldName(afield)});
            } else {
                td = new TupleDesc(new Type[]{afieldType}, new String[]{tup.getTupleDesc().getFieldName(afield)});
            }
        }
        if (what == Op.COUNT) {
            count(tup);
        } 
    }
    
    private void count(Tuple tup) {
        if (gbfieldType == null) {
            groups.merge(PRESENT, 1, Integer::sum);
        } else {
            groups.merge(tup.getField(gbfield), 1, Integer::sum);
        }
    }

    @Override
    public Iterable<Tuple> getTuples() {
        tuples.clear();
        for (Map.Entry<Field, Integer> entry : groups.entrySet()) {
            Field key = entry.getKey();
            Integer value = entry.getValue();
            // System.out.println(key.toString() + " " + value.toString());
            Tuple tup = new Tuple(td);
            if (gbfieldType != null) {
                tup.setField(0, key);
                tup.setField(1, new IntField(value));
            } else {
                tup.setField(0, new IntField(value));
            }
            tuples.add(tup);
        }
        return tuples;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }
}

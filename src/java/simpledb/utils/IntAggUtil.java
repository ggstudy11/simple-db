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

public class IntAggUtil implements AggUtil{
    private final Map<Field, int[]> groups;
    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private static final Type afieldType = Type.INT_TYPE;
    private static final Field PRESENT = new IntField(-1);
    private final Op what;
    private List<Tuple> tuples = new ArrayList<>();
    private TupleDesc td;
    
    public IntAggUtil(Map<Field, int[]> groups, int gbfield, Type gbfieldType, int afield, Op what) {
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
        switch (what) {
            case SUM:
                sum(tup);
                break;
            case COUNT:
                count(tup);
                break;
            case MAX:
                max(tup);
                break;
            case MIN:
                min(tup);
                break;
            case AVG:
                avg(tup);
                break;
        }
    }

    private void sum(Tuple tup) {
        int[] val = null;
        if (gbfieldType == null) {
            val = groups.get(PRESENT);
        } else {
            val = groups.get(tup.getField(gbfield));
        }
        if (val == null) {
            val = new int[1];
        }
        val[0] += ((IntField) tup.getField(afield)).getValue();
        groups.put(gbfieldType == null ? PRESENT : tup.getField(gbfield), val);
    }

    private void count(Tuple tup) {
        int[] val = null;
        if (gbfieldType == null) {
            val = groups.get(PRESENT);
        } else {
            val = groups.get(tup.getField(gbfield));
        }
        if (val == null) {
            val = new int[1];
        }
        val[0] += 1;
        groups.put(gbfieldType == null ? PRESENT : tup.getField(gbfield), val);
    }

    private void max(Tuple tup) {
        int[] val = null;
        if (gbfieldType == null) {
            val = groups.get(PRESENT);
        } else {
            val = groups.get(tup.getField(gbfield));
        }
        if (val == null) {
            val = new int[1];
        }
        val[0] = Math.max(val[0], ((IntField) tup.getField(afield)).getValue());
        groups.put(gbfieldType == null ? PRESENT : tup.getField(gbfield), val);
    }

    private void min(Tuple tup) {
        int[] val = null;
        if (gbfieldType == null) {
            val = groups.get(PRESENT);
        } else {
            val = groups.get(tup.getField(gbfield));
        }
        if (val == null) {
            val = new int[1];
            val[0] = Integer.MAX_VALUE;
        }
        val[0] = Math.min(val[0], ((IntField) tup.getField(afield)).getValue());
        groups.put(gbfieldType == null ? PRESENT : tup.getField(gbfield), val);
    }

    private void avg(Tuple tup) {
        int[] val = null;
        if (gbfieldType == null) {
            val = groups.get(PRESENT);
        } else {
            val = groups.get(tup.getField(gbfield));
        }
        if (val == null) {
            val = new int[2];
        }
        val[0] = (val[1] * val[0] + ((IntField) tup.getField(afield)).getValue()) / (val[1] + 1);
        val[1]++;
        groups.put(gbfieldType == null ? PRESENT : tup.getField(gbfield), val);
    }

    @Override
    public Iterable<Tuple> getTuples() {
        tuples.clear();
        for (Map.Entry<Field, int[]> entry : groups.entrySet()) {
            Field key = entry.getKey();
            int[] value = entry.getValue();
            Tuple tup = new Tuple(td);
            if (gbfieldType != null) {
                tup.setField(0, key);
                tup.setField(1, new IntField(value[0]));
            } else {
                tup.setField(0, new IntField(value[0]));
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

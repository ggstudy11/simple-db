package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableid;
    private final int ioCostPerPage;
    private final Map<String, IntHistogram> intHistograms;
    private final Map<String, StringHistogram> stringHistograms;
    private int numTuple;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        DbFile f = Database.getCatalog().getDatabaseFile(tableid);
        DbFileIterator iterator = f.iterator(new TransactionId());
        TupleDesc tupleDesc = f.getTupleDesc();
        this.tableid = tableid;
        intHistograms = new HashMap<>();
        stringHistograms = new HashMap<>();
        this.ioCostPerPage = ioCostPerPage;
        int[] min = new int[tupleDesc.numFields()];
        int[] max = new int[tupleDesc.numFields()];
        Arrays.fill(min, Integer.MAX_VALUE);
        Arrays.fill(max, Integer.MIN_VALUE);
        try {
            iterator.open();
            while (iterator.hasNext()) {;
                numTuple++;
                Tuple t = iterator.next();
                for (int i = 0; i < tupleDesc.numFields(); ++i) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        min[i] = Math.min(min[i], ((IntField) t.getField(i)).getValue());
                        max[i] = Math.max(max[i], ((IntField) t.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            System.out.println("something wrong happened : " + e);
        }
        for (int i = 0; i < tupleDesc.numFields(); ++i) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                intHistograms.put(tupleDesc.getFieldName(i), new IntHistogram(NUM_HIST_BINS, min[i], max[i]));
            } else {
                stringHistograms.put(tupleDesc.getFieldName(i), new StringHistogram(NUM_HIST_BINS));
            }
        }
        try {
            iterator.rewind();
            while (iterator.hasNext()) {
                Tuple t = iterator.next();
                for (int i = 0; i < tupleDesc.numFields(); ++i) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram intHistogram = intHistograms.get(tupleDesc.getFieldName(i));
                        intHistogram.addValue(((IntField)t.getField(i)).getValue());
                    } else {
                        StringHistogram stringHistogram = stringHistograms.get(tupleDesc.getFieldName(i));
                        stringHistogram.addValue(((StringField)t.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            System.out.println("something wrong happened : " + e);
        } finally {
            iterator.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ((HeapFile)Database.getCatalog().getDatabaseFile(tableid)).numPages() * ioCostPerPage * 2;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (Database.getCatalog().getTupleDesc(tableid).getFieldType(field) == Type.INT_TYPE) {
            return intHistograms.get(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().getFieldName(field)).avgSelectivity();
        } else {
            return stringHistograms.get(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().getFieldName(field)).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (Database.getCatalog().getTupleDesc(tableid).getFieldType(field) == Type.INT_TYPE) {
            return intHistograms.get(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().getFieldName(field)).
                    estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
            return stringHistograms.get(Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().getFieldName(field)).
                    estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return numTuple;
    }

}

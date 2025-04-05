package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    private final TransactionId tid;
    private final OpIterator child;
    private final int tabledId;
    private OpIterator[] children;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tabledId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"effected num"});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!child.hasNext()) return null;
        Tuple tup = new Tuple(getTupleDesc());
        int effected_num = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tabledId, child.next());
                effected_num++;
            } catch (IOException e) {
                System.out.println("error msg: " + e);
            }
        }
        tup.setField(0, new IntField(effected_num));
        return tup;
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }
}

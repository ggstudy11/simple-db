package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    private final JoinPredicate p;
    private OpIterator[] children;
    private Tuple t1;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p = p;
        children = new OpIterator[]{child1, child2};
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        return children[0].getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        return children[1].getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        children[0].open();
        children[1].open();
    }

    public void close() {
        super.close();
        children[0].close();
        children[1].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        t1 = null;
        children[0].rewind();
        children[1].rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while ((children[0].hasNext() || t1 != null) && children[1].hasNext()) {
            if (t1 == null) {
                t1 = children[0].next();
            }
            while (children[1].hasNext()) {
                Tuple t2 = children[1].next();
                if (p.filter(t1, t2)) {
                    TupleDesc td = getTupleDesc();
                    Tuple tuple = new Tuple(td);
                    int len1 = children[0].getTupleDesc().numFields();
                    int len2 = children[1].getTupleDesc().numFields();
                    for (int i = 0; i < len1; ++i) {
                        tuple.setField(i, t1.getField(i));
                    }
                    for (int i = len1; i < len1 + len2; ++i) {
                        tuple.setField(i, t2.getField(i - len1));
                    }
                    return tuple;
                }
            }
            children[1].rewind();
            t1 = null;
        }
        return null;
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

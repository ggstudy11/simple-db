package simpledb.common;

import simpledb.transaction.TransactionId;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author ggstudy11
 * @date Created in 4/12/2025 12:46 PM
 * @description 页锁
 */
public class LockItem {
    private Set<TransactionId> tids;
    private Permissions permit;
    public LockItem(TransactionId tid, Permissions permit) {
        tids = Collections.synchronizedSet(new HashSet<>());
        tids.add(tid);
        this.permit = permit;
    }
    public Set<TransactionId> getTids() {
        return tids;
    }
    public Permissions getPermit() {
        return permit;
    }
    public void addTransactionId(TransactionId tid) {
        tids.add(tid);
    }
    public void setPermit(Permissions permit) {
        this.permit = permit;
    }
    public void remove(TransactionId tid) {
        tids.remove(tid);
    }
    public boolean isEmpty() {
        return tids.isEmpty();
    }
    public boolean containsTid(TransactionId tid) {
        return tids.contains(tid);
    }
    public void resetTids() {
        tids.clear();
    }
}

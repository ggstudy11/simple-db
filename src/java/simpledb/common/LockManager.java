package simpledb.common;


import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author ggstudy11
 * @date Created in 4/12/2025 12:46 PM
 * @description 全局锁管理器
 */
public class LockManager {
    ConcurrentHashMap<PageId, LockItem> locks = new ConcurrentHashMap<>();
    Map<TransactionId, Set<TransactionId>> waitingGraph = new HashMap<>();

    public synchronized boolean requireLock(PageId pid, TransactionId tid, Permissions permit) throws TransactionAbortedException{
        printInfo();
        if (locks.get(pid) == null) {
            locks.put(pid, new LockItem(tid, permit));
            return true;
        }
        LockItem lockItem = locks.get(pid);
        if (lockItem.getTids().contains(tid) && lockItem.getTids().size() == 1) {
            if (lockItem.getPermit() == Permissions.READ_ONLY && permit == Permissions.READ_WRITE) {
                lockItem.setPermit(Permissions.READ_WRITE);
            }
            return true;
        }
        if (lockItem.getPermit() == Permissions.READ_ONLY && permit == Permissions.READ_ONLY) {
            lockItem.addTransactionId(tid);
            return true;
        }

        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> recStack = new HashSet<>();
        Set<TransactionId> waitSet = new HashSet<>(lockItem.getTids());
        waitingGraph.put(tid, waitSet);
        if (checkIfDead(tid, visited, recStack)) {
            throw new TransactionAbortedException();
        }
        return false;
    }

    public synchronized void tryLock(PageId pid, TransactionId tid, Permissions permit) throws TransactionAbortedException{
        while (!requireLock(pid, tid, permit)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                System.out.println("Something wrong happened!");
            }
        }
        waitingGraph.remove(tid);
    }


    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        Set<PageId> toRemove = new HashSet<>();
        locks.compute(pid, (key, lockItem) -> {
            if (lockItem == null)
                return null;
            lockItem.remove(tid);
            if (lockItem.isEmpty()) {
                toRemove.add(key);
            }
            return lockItem;
        });
        waitingGraph.remove(tid);
        this.notifyAll();
        for (PageId pageId : toRemove) {
            releaseLock(pageId);
        }
    }

    public boolean holdsLock(PageId pid, TransactionId tid) {
        if (locks.get(pid) == null) return false;
        return locks.get(pid).containsTid(tid);
    }

    public synchronized void releaseLock(PageId pid) {
        locks.remove(pid);
    }

    public synchronized void releaseLock(TransactionId tid) {
        for (Map.Entry<PageId, LockItem> entry : locks.entrySet()) {
            releaseLock(entry.getKey(), tid);
        }
    }

    public void printInfo() {
        for (Map.Entry<PageId, LockItem> entry : locks.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue().getTids() + " " + entry.getValue().getPermit());
        }
    }

    public synchronized boolean checkIfDead(TransactionId src, Set<TransactionId> visited, Set<TransactionId> recStack) {
        if (recStack.contains(src)) return true;
        if (visited.contains(src)) return false;
        recStack.add(src);
        visited.add(src);
        if (waitingGraph.get(src) == null) return false;
        for (TransactionId tid : waitingGraph.get(src)) {
            if (checkIfDead(tid, visited, recStack))
                return true;
        }
        return false;
    }
}

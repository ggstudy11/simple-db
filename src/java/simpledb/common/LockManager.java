package simpledb.common;


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
    Map<PageId, LockItem> locks = new HashMap<>();
    final ConcurrentHashMap<TransactionId, Set<TransactionId>> waitingGraph = new ConcurrentHashMap<>();

    public synchronized boolean tryLock(PageId pid, TransactionId tid, Permissions permit) throws TransactionAbortedException{
        // printInfo();
        // 如果没有锁则上锁
        if (locks.get(pid) == null) {
            locks.put(pid, new LockItem(tid, permit));
            return true;
        }
        // 如果有锁则进一步检查
        LockItem lockItem = locks.get(pid);
        // 如果独占共享锁可以进行锁升级
        if (lockItem.getTids().contains(tid) && lockItem.getTids().size() == 1) {
            if (lockItem.getPermit() == Permissions.READ_ONLY && permit == Permissions.READ_WRITE) {
                lockItem.setPermit(Permissions.READ_WRITE);
            }
            return true;
        }
        // 如果是共享锁可以共享
        if (lockItem.getPermit() == Permissions.READ_ONLY && permit == Permissions.READ_ONLY) {
            lockItem.addTransactionId(tid);
            return true;
        }

        // 否则进入阻塞等待 更新等待图 检查是否死锁
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> recStack = new HashSet<>();
        Set<TransactionId> waitSet = new HashSet<>(lockItem.getTids());
        waitingGraph.put(tid, waitSet); // tid -> waitSet(占有锁的tid)
        if (checkIfDead(tid, visited, recStack)) {
            // 如果死锁则抛出异常
            throw new TransactionAbortedException();
        }
        return false;
    }

    public synchronized void Lock(PageId pid, TransactionId tid, Permissions permit) throws TransactionAbortedException {
        while (!tryLock(pid, tid, permit)) {
            try {
                // 进入等待
                this.wait(1000);
            } catch (InterruptedException e) {
                System.out.println("Something wrong happened!");
            }
        }
        // 获取到资源后移除等待图节点
        waitingGraph.remove(tid);
    }

    /* 释放某一页上的某个tid的锁 */
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        LockItem lockItem = locks.get(pid);
        if (lockItem.containsTid(tid)) {
            lockItem.remove(tid);
        }
        if (lockItem.isEmpty()) {
            locks.remove(pid);
        }
    }

    /* 是否占有锁 */
    public boolean holdsLock(PageId pid, TransactionId tid) {
        if (locks.get(pid) == null) return false;
        return locks.get(pid).containsTid(tid);
    }

    /* 是否某一页上的所有锁 */
    public synchronized void releaseLock(PageId pid) {
        locks.remove(pid);
    }

    /* 释放某事务的所有锁 */
    public synchronized void releaseLock(TransactionId tid) {
        List<PageId> toRemove = new ArrayList<>();
        for (Map.Entry<PageId, LockItem> entry : locks.entrySet()) {
            if (holdsLock(entry.getKey(), tid)) {
                toRemove.add(entry.getKey());
            }
        }
        for (PageId pid : toRemove) {
            releaseLock(pid, tid);
        }

        waitingGraph.remove(tid);
    }

    /* for debug */
    public void printInfo() {
        for (Map.Entry<PageId, LockItem> entry : locks.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue().getTids() + " " + entry.getValue().getPermit());
        }
    }

    public  boolean checkIfDead(TransactionId src, Set<TransactionId> visited, Set<TransactionId> recStack) {
        if (recStack.contains(src)) return true;
        if (visited.contains(src)) return false;
        recStack.add(src);
        visited.add(src);
        if (waitingGraph.get(src) == null) return false;
        for (TransactionId tid : waitingGraph.get(src)) {
            if (checkIfDead(tid, visited, recStack))
                return true;
        }
        recStack.remove(src);
        return false;
    }
}

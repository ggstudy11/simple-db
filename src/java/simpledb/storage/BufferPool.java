package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    final LRUCache<PageId, Page> pages;
    final LockManager lockManager;

    public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        // 构造函数，初始化缓存容量
        public LRUCache(int capacity) {
            // 调用父类构造函数，设置初始容量、负载因子和访问顺序
            super(capacity, 0.75f, true);
            this.capacity = capacity;
        }


        public boolean notEmpty() {
            return capacity == size();
        }
    }

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new LRUCache<>(numPages);
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // hint: use DbFile.readPage() to access Page of a DbFile
       lockManager.Lock(pid, tid, perm);
       if (!pages.containsKey(pid)) {
            if (pages.notEmpty()) {
                evictPage();
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            pages.put(pid, file.readPage(pid));
       }
       return pages.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
                    Page page = entry.getValue();
                    if (page.isDirty() == tid) {
                        flushPage(page.getId());
                        page.setBeforeImage();
                    }
                }
            } catch (IOException e) {
                System.out.println("Something Wrong happened!");
            }
        } else {
            restorePages(tid);
        }
        lockManager.releaseLock(tid);
    }

     private void restorePages(TransactionId tid) {
         Set<PageId> pidToUpdate = new HashSet<>();
         for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
             if (!tid.equals(entry.getValue().isDirty())) continue;
             pidToUpdate.add(entry.getKey());
         }
         for (PageId pid : pidToUpdate) {
             DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
             Page page = file.readPage(pid);
             pages.put(pid, page);
         }
     }
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = f.insertTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            this.pages.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = f.deleteTuple(tid, t);
        for (Page p : pages) {
            this.pages.put(p.getId(), p);
            p.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Set<Page> s = new HashSet<>();
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page page = entry.getValue();
            s.add(page);
        }
        for (Page p : s) {
            flushPage(p.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);
        if (page.isDirty() == null) return;
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
        Database.getLogFile().force();
        file.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Set<Page> pagesToChange = new HashSet<>();
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                pagesToChange.add(page);
            }
        }
        for (Page p : pagesToChange) {
            DbFile file = Database.getCatalog().getDatabaseFile(p.getId().getTableId());
            file.writePage(p);
            p.markDirty(false, null);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if (entry.getValue().isDirty() != null) {
                continue;
            }
            removePage(entry.getKey());
            return;
        }
        throw new DbException("no page to evict");
    }

}

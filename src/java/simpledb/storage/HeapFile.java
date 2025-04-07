package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ArrayList;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {


    private final File f;
    private final TupleDesc td;

    private class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private int pgNo = 0;
        private HeapPageId pid = new HeapPageId(getId(), pgNo);
        private Iterator<Tuple> iterator;
        private boolean flag = false;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!flag) return false;
            if (iterator.hasNext()) return true;
            while (pgNo < numPages() - 1) {
                pid = new HeapPageId(getId(), ++pgNo);
                iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
                if (iterator.hasNext())
                    return true;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (hasNext()) {
                return iterator.next();
            }
            throw new NoSuchElementException("no valid element!");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            flag = true;
            iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pgNo = 0;
            pid = new HeapPageId(getId(), pgNo);
            iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void close() {
            flag = false;
        }
    }
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // 偏移量
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        Page page = null;
        // Raf 随机访问文件
        // Raf seek 跳过 offset字节数
        // Raf read 读满 data数组
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")){
            raf.seek(offset);
            raf.read(data);
            page = new HeapPage((HeapPageId)pid, data);
        } catch(IOException e) {
            
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            long offset = page.getId().getPageNumber() * BufferPool.getPageSize();
            byte[] data = page.getPageData();
            raf.seek(offset);
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> l = new ArrayList<>();
        for (int i = 0; i < numPages(); ++i) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumUnusedSlots() == 0)
                continue;
            page.insertTuple(t);
            l.add(page);
            return l;
        }
        // 如果找不到空闲页则新建 append写入
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f, true))) {
            byte[] empty = HeapPage.createEmptyPageData();
            bos.write(empty);
        }
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        l.add(page);
        return l;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        List<Page> l = new ArrayList<>();
        l.add(page);
        return l;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}


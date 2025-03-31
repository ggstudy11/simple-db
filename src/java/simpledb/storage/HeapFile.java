package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
        private int nextPgNo;
        private HeapPageId pid;
        private Iterator<Tuple> iterator;
        private Tuple next = null;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.nextPgNo = 0;
            this.pid = new HeapPageId(getId(), nextPgNo);
        }
        
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (next == null) next = readNext();
            return next != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) throw new NoSuchElementException();
            }

            Tuple result = next;
            next = null;
            return result;
        }

        private Tuple readNext() throws DbException, TransactionAbortedException{
            if (iterator == null) return null;
            if (iterator.hasNext()) 
                return iterator.next();
            else {
                nextPgNo++;
                if (nextPgNo == numPages()) {
                    return null;
                }
                this.pid = new HeapPageId(getId(), nextPgNo);
                this.iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
                if (iterator.hasNext()) return iterator.next();
                return null;
            }
            
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.nextPgNo = 0;
            this.pid = new HeapPageId(getId(), nextPgNo);
            this.iterator = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void close() {
            next = null;
            iterator = null;
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
        // TODO: some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
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
        // TODO: some code goes here
        // throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        HeapPage page = null;
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")){
            raf.seek(offset);
            raf.read(data);
            page = new HeapPage((HeapPageId)pid, data);
        } catch(IOException e) {
            
        }
        return page;
        // TODO: some code goes here
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int)f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(tid);
    }

}


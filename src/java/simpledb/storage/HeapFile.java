package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.index.BTreeRootPtrPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    /**
     * 实现一个HeapPage的迭代器
     */
    public static  final class heapPageIterator implements DbFileIterator{

        private HeapFile heapFile;
        final TransactionId tid;
        private int nextPageNo;
        private Iterator<Tuple> tupleIterator;
        private Tuple next = null;

        public heapPageIterator(TransactionId tid,final HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
            this.nextPageNo = 0;
            tupleIterator = null;
        }

        public boolean hasNext() throws TransactionAbortedException, DbException {
            if(next == null) next = this.readNext();
            return next != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (next == null) {
                next = readNext();
                if (next == null) throw new NoSuchElementException();
            }

            Tuple result = next;
            next = null;
            return result;
        }

        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null) return null;
            if(tupleIterator.hasNext()){
                return this.tupleIterator.next();
            }else if(this.nextPageNo < this.heapFile.numPages()){
                this.tupleIterator = getNextPageIterator();
                return readNext();
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.tupleIterator = getNextPageIterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close(){
            next = null;
            this.nextPageNo = 0;
            this.tupleIterator = null;
        }

        private Iterator<Tuple> getNextPageIterator() throws TransactionAbortedException, DbException {
            return getNextPage().iterator();
        }

        private HeapPage getNextPage() throws TransactionAbortedException, DbException {
            HeapPageId pageId = new HeapPageId(this.heapFile.getId(),this.nextPageNo);
            this.nextPageNo++;
            return (HeapPage) Database.getBufferPool().getPage(this.tid,pageId,Permissions.READ_ONLY);
        }

    }


    private File f;
    private TupleDesc td;
    //private DataInputStream dataInputStream;（旧）
    private RandomAccessFile randomAccessFile;
    private int pageNum;
    //private List<byte[]> allPageData; (旧)
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.pageNum = (int) Math.ceil((double) this.f.length() / BufferPool.getPageSize());
        /**
         * 将所有字节流烤出
         */
        try {
            this.randomAccessFile = new RandomAccessFile(f,"rw");
            //test  === > 传进来的文件没问题
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        /**
         * 将本文件注册到Catalog
         */
        Database.getCatalog().addTable(this);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.f.hashCode() + this.td.hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        /**
         * 这个地方应当将f字节流存储起来，如果过大 应当分页存取，再返回对应字符页面
         */
        // 用输入流来读取文件，读取内容为一页内容， 具体对应的pageNum的内容
        //首先得到对应的pageNum
        int pageNum = pid.getPageNumber();
        byte[] targetPageData = new byte[BufferPool.getPageSize()];
        //targetPageData = allPageData.get(pageNum);
        long pointPosi = pageNum * BufferPool.getPageSize();
        try {
            if(pid.getPageNumber() == this.pageNum){
                //说明需要新建一个页码
                HeapPage heapPage = new HeapPage((HeapPageId) pid,new byte[BufferPool.getPageSize()]);
//                writePage(heapPage);  ==== >  不需要在这里刷盘，所有刷盘操作应该让bufferpool来做
                this.pageNum++;
                return heapPage;
            }else{
                //设置文件流读写指针位置
                randomAccessFile.seek(pointPosi);
                randomAccessFile.read(targetPageData,0,BufferPool.getPageSize());
                Page targetPage = new HeapPage((HeapPageId) pid,targetPageData);
                return targetPage;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        //定位指针
        PageId pid = page.getId();
        this.pageNum = Math.max(this.pageNum,pid.getPageNumber() + 1);
        long pointPosi = pid.getPageNumber() * BufferPool.getPageSize();  //===================> 导致bug
        this.randomAccessFile.seek(pointPosi);
        this.randomAccessFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pageNum;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //为该tuple找一个灵魂归蜀之地
        List<Page> lst = new ArrayList<>();
        for (int i = 0; i < this.pageNum; i++) {
            HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(this.getId(),i),Permissions.READ_ONLY);
            //满了就下一个
            if(tempPage.getNumEmptySlots() == 0) continue;
            tempPage.insertTuple(t);
            lst.add(tempPage);
        }
        if(lst.size() == 0){
            //说明页满了，增加一个新的页码
            //HeapPage newPage = new HeapPage(new HeapPageId(this.getId(),this.pageNum),new byte[BufferPool.getPageSize()]);
//            writePage(newPage);
//            // newPage.insertTuple(t); =====> newPage已经不是该文件里那个位置了，因为他作为字节流被写入到this.f中了
//            HeapPage targetPage = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(this.getId(),this.pageNum - 1),Permissions.READ_ONLY);
//            targetPage.insertTuple(t);
//            lst.add(newPage);
            PageId pageId = new HeapPageId(this.getId(),this.pageNum);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
            heapPage.insertTuple(t);
            lst.add(heapPage);
        }
        return lst;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //首先找到要删除Tuple的所在页面
        PageId pageId = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_ONLY);
        heapPage.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(heapPage));
    }

    // see DbFile.java for javadocs

    /**
     * 不会做
     * @param tid
     * @return
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        /**
         *
         * 旧版本臭代码，====》 思想：将所有tuple都拼接到一个list里面去，然后传给迭代器进行封装
         *  BufferPool bufferPool = Database.getBufferPool();
         *         //自己的pageId，自己要创建  tableId = this.getId()   pageNum = 偏移量
         *         int pageNum = numPages();
         *         //创建一个拼接各个iterator
         *         List<Tuple> lst = new ArrayList<>();
         *         for(int i = 0; i < pageNum ;i ++){
         *             PageId pageId = HeapPageId.getPageId(this.getId(),i);
         *             //System.out.println(i + "页面个数为" + numPages() + "文件长度为" + f.length());
         *             try {
         *                 //由于是要迭代的，因此permission为read_only
         *                 HeapPage page = (HeapPage) bufferPool.getPage(tid,pageId,Permissions.READ_ONLY);
         *                 for (Iterator<Tuple> it = page.iterator(); it.hasNext(); ) {
         *                     Tuple tuple = it.next();
         *                     lst.add(tuple);
         *                 }
         *             } catch (TransactionAbortedException e) {
         *                 e.printStackTrace();
         *             } catch (DbException e) {
         *                 e.printStackTrace();
         *             }
         *         }
         *         return new heapPageIterator(tid, lst.iterator());
         */

        //新版本代码===》 实现了高度解耦 ===》只给迭代器文件，具体内容自己处置
        return new heapPageIterator(tid,this);

    }

}


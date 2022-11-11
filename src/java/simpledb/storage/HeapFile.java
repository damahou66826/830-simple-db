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

        Iterator<Tuple> it = null;
        // 临时迭代器存储构造方法传过来的iterator
        //Iterator<Tuple> temp = null;
        private List<Tuple> temp;
        final TransactionId tid;
        private Tuple next = null;

        public heapPageIterator(TransactionId tid,Iterator<Tuple> it) {
            this.tid = tid;
            this.temp = new ArrayList<>();
            while(it.hasNext()){
                this.temp.add(it.next());
            }
        }

        public boolean hasNext(){
            if(it == null) return false;
            return it.hasNext();
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
            if (it != null && !it.hasNext())
                it = null;

            if (it == null)
                return null;
            return it.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.it = temp.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close(){
            next = null;
            it = null;
        }

    }


    private File f;
    private TupleDesc td;
    private DataInputStream dataInputStream;
    private List<byte[]> allPageData;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.allPageData = new ArrayList<>();
        /**
         * 将所有字节流烤出
         */
        try {
            this.dataInputStream = new DataInputStream(new FileInputStream(f));
            int pageSize = BufferPool.getPageSize();
            byte[] pageData = new byte[pageSize];
            while(true){
                try {
                    int flag = this.dataInputStream.read(pageData,0,pageSize);
                    allPageData.add(pageData.clone());
                    if(flag == -1) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
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
//        int pageSize = BufferPool.getPageSize();
//        byte[] pageData = new byte[pageSize];
//        try {
//            // !!!! 防止最后一页指针越界  加入Math.min（） 来进行规整  (去文档查read的参数作用)
//            //System.out.println(f.length()-pageSize*pageNum + "   pageSize = " +pageSize);
//            // 这个地方有问题
//            dataInputStream.read(pageData,0, (int) Math.min(pageSize,(f.length()-pageSize*pageNum)));
//            Page target = new HeapPage((HeapPageId) pid,pageData);
//            return target;
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        byte[] targetPageData = allPageData.get(pageNum);
        try {
            Page targetPage = new HeapPage((HeapPageId) pid,targetPageData);
            return targetPage;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 文件大小除以每个页面大小 往上取整
        int pageSize = BufferPool.getPageSize();
        return (int) Math.ceil((double) f.length() / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs

    /**
     * 不会做
     * @param tid
     * @return
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        BufferPool bufferPool = new BufferPool(numPages());
        //自己的pageId，自己要创建  tableId = this.getId()   pageNum = 偏移量
        int pageNum = numPages();
        //创建一个拼接各个iterator
        List<Tuple> lst = new ArrayList<>();
        for(int i = 0; i < pageNum ;i ++){
            PageId pageId = HeapPageId.getPageId(this.getId(),i);
            //System.out.println(i + "页面个数为" + numPages() + "文件长度为" + f.length());
            try {
                //由于是要迭代的，因此permission为read_only
                HeapPage page = (HeapPage) bufferPool.getPage(tid,pageId,Permissions.READ_ONLY);
                for (Iterator<Tuple> it = page.iterator(); it.hasNext(); ) {
                    Tuple tuple = it.next();
                    lst.add(tuple);
                }
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        return new heapPageIterator(tid, lst.iterator());
    }

}


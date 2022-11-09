package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.index.BTreeHeaderPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

//    private static final class bufferPoolPage{
//        private final TransactionId transactionId;
//        private final PageId pageId;
//        private final Permissions permissions;
//
//        public bufferPoolPage(TransactionId transactionId, PageId pageId, Permissions permissions) {
//            this.transactionId = transactionId;
//            this.pageId = pageId;
//            this.permissions = permissions;
//        }
//
//        /**
//         * 重写equal 与 hashcode
//         */
//        @Override
//        public int hashCode() {
//            return transactionId.hashCode() + pageId.hashCode() + permissions.hashCode();
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            bufferPoolPage test = (bufferPoolPage) o;
//            if(transactionId.equals(test.transactionId) &&
//                    pageId.equals(test.pageId) &&
//                    permissions.equals(test.permissions)
//                ) return true;
//            return false;
//        }
//    }

    /**
     *
     * 维护一个空闲链表与忙碌双端队列，LRU算法进行淘汰
     * 维护一个hashmap，对应好其中的关系 ,hashmap要加锁，防止多线程访问时候出问题
     */
    private volatile HashMap<PageId,Page> map = new HashMap<>();
    //创建一个读写锁
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    Deque<Page> busyDeque;
    Deque<Page> freeDeque;
    private int realNumPage = DEFAULT_PAGES;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        busyDeque = new LinkedList<>();
        freeDeque = new LinkedList<>();
        this.realNumPage = numPages;
        /**
         * 初始化对应数量的页加入freeDeque
         * 
         */
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        readWriteLock.readLock().lock();
        try {
            if(map.containsKey(pid)){
                return map.get(pid);
            }
        }catch(Exception e){
            throw new TransactionAbortedException();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
        // 11 - 8  如果没有去catalog中拿dbfile
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page =  dbFile.readPage(pid);

        /**
         * 没有的桦需要创建和插入
         */
        if(busyDeque.size() < realNumPage){
            busyDeque.addFirst(page);
            //写入map
            readWriteLock.writeLock().lock();
            try {
                map.put(pid,page);
                return page;
            }catch(Exception e){
                throw new TransactionAbortedException();
            }
            finally {
                readWriteLock.writeLock().unlock();
            }
        }

        /**
         * 否则的需要删除掉尾部节点，再进行插入
         */
        //写入map
        readWriteLock.writeLock().lock();
        try {
            //寻找要删除掉的key
            for(Map.Entry<PageId,Page> entry : map.entrySet()){
                if(entry.getValue().equals(page)){
                    map.remove(entry.getKey());
                    break;
                }
            }
            //加入到map中
            map.put(pid,page);
        }catch(Exception e){
            throw new TransactionAbortedException();
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
        busyDeque.removeLast();
        busyDeque.addFirst(page);
        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();

        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

}

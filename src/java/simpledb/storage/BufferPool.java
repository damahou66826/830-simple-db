package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private class pageIdNode{
        PageId key;
        Page value;
        pageIdNode pre;
        pageIdNode next;

        public pageIdNode(){
            this.pre = null;
            this.next = null;
        }

        public pageIdNode(PageId pageId,Page page){
            this.key = pageId;
            this.value = page;
            this.pre = null;
            this.next = null;
        }
    }
    private pageIdNode start;
    private pageIdNode end;

    private volatile Map<PageId,pageIdNode> pageIdToPageIdNode;

    private int realNumPage = DEFAULT_PAGES;

    //private volatile Map<PageId,Page> pageIdToPage;

    private volatile Map<TransactionId,Set<PageId>> transactionIdToPageIdSet;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        transactionIdToPageIdSet = new HashMap<>();
        this.realNumPage = numPages;

        /**
         * 初始化lru链表
         */
        this.start = new pageIdNode();
        this.end = new pageIdNode();
        this.start.next = end;
        this.end.pre = start;
        pageIdToPageIdNode = new HashMap<>();

    }

    public void deleteNode(pageIdNode node){
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.next = null;
        node.pre = null;
    }

    public void addToStart(pageIdNode node){
        node.next = start.next;
        node.next.pre = node;
        node.pre = start;
        start.next = node;
    }

    public void moveToStart(pageIdNode node){
        deleteNode(node);
        addToStart(node);
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
        Set<PageId> pids = transactionIdToPageIdSet.getOrDefault(tid,new HashSet<>());
        pids.add(pid);
        transactionIdToPageIdSet.put(tid,pids);

        if(pageIdToPageIdNode.containsKey(pid)){
            Page page  = pageIdToPageIdNode.get(pid).value;
            moveToStart(pageIdToPageIdNode.get(pid));
            return page;
        }

        if(pageIdToPageIdNode.size() >= this.realNumPage){
            //每次移除队尾页面
            evictPage();
        }

        Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pageIdNode newNode = new pageIdNode(pid,newPage);
        pageIdToPageIdNode.put(pid,newNode);
        //添加新页面，直接add
        addToStart(newNode);

        return newPage;
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
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = dbFile.insertTuple(tid,t);
        if(dirtiedPages == null) return;
        for (Page dirtiedPage : dirtiedPages){
            //访问过，则移动
            moveToStart(pageIdToPageIdNode.get(dirtiedPage.getId()));
            dirtiedPage.markDirty(true,tid);
        }
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId =  t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = dbFile.deleteTuple(tid,t);

        for (Page dirtiedPage : dirtiedPages){
            //访问过 则移动
            moveToStart(pageIdToPageIdNode.get(dirtiedPage.getId()));
            dirtiedPage.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId : this.pageIdToPageIdNode.keySet()){
            flushPage(pageId);
        }
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
        this.pageIdToPageIdNode.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.pageIdToPageIdNode.get(pid).value;
        if(page != null && page.isDirty() != null){
            //如果该页不为空并且该页是脏的
            //应当调用对应文件的write方法来写进去新的page
            DbFile heapFile =  Database.getCatalog().getDatabaseFile(pid.getTableId());
            heapFile.writePage(page);
            page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIds = this.transactionIdToPageIdSet.get(tid);
        if(pageIds.size() == 0){
            //说明该事务在缓冲池中没有放页码
            return;
        }
        for(PageId pageId : pageIds){
            flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
//        boolean flag = false;
//        for(Page page : this.pageIdToPage.values()){
//            if(page.isDirty() != null){
//                //如果该页面是脏的，优先保留
//                continue;
//            }
//            //如果不脏
//            try {
//                this.flushPage(page.getId());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            flag = true;
//            discardPage(page.getId());
//            break;
//        }
//
//        if(!flag){
//            //说明页面全部都是脏的，这时候需要寻找一个强制刷页
//            //这里选择随机选择一个事务，刷页
//            for(TransactionId transactionId : this.transactionIdToPageIdSet.keySet()){
//                if(this.transactionIdToPageIdSet.get(transactionId).size() > 0){
//                    //如果该事务的pageset不为空，刷页！！
//                    try {
//                        flushPages(transactionId);
//                        evictPage();  //再去尝试丢弃页
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }

        //移除掉最后一页
        pageIdNode pageIdNode = end.pre;
        Page page = pageIdNode.value;
        if(page.isDirty() != null){
            try {
                this.flushPage(page.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        discardPage(page.getId());
    }

}

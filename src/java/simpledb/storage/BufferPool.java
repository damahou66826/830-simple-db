package simpledb.storage;

import lombok.val;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.TransactionLockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

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

    private Lock lock;


    private static class pageIdNode{
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

        public boolean equal(pageIdNode newNode){
            return newNode.key.equals(this.key) && newNode.value.equals(this.value);
        }

        public int hashcode(){
            return this.key.hashCode() + this.value.hashCode();
        }

    }

    private final pageIdNode start;
    private final pageIdNode end;

    private final Map<PageId,pageIdNode> pageIdToPageIdNode;

    private int realNumPage = DEFAULT_PAGES;

    private final Map<TransactionId,Set<PageId>> transactionIdToPageIdSet;

    private TransactionLockManager transactionLockManager = null;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.realNumPage = numPages;

        /**
         * 初始化lru链表
         */
        this.start = new pageIdNode();
        this.end = new pageIdNode();
        this.start.next = end;
        this.end.pre = start;
        this.pageIdToPageIdNode = new ConcurrentHashMap<>();
        this.transactionIdToPageIdSet = new ConcurrentHashMap<>();
        this.transactionLockManager = new TransactionLockManager();
        this.lock = new ReentrantLock();
    }

    public synchronized void deleteNode(pageIdNode node){
        //System.out.println(node.pre);
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.next = null;
        node.pre = null;
    }

    public synchronized void addToStart(pageIdNode node){
        node.next = start.next;
        node.next.pre = node;
        node.pre = start;
        start.next = node;
    }

    public synchronized void moveToStart(pageIdNode node){
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
        this.transactionLockManager.lock(tid, pid, perm);
        if(!transactionIdToPageIdSet.containsKey(tid)){
            transactionIdToPageIdSet.put(tid,new HashSet<PageId>());
        }
        Set<PageId> pids = transactionIdToPageIdSet.get(tid);
        pids.add(pid);
        transactionIdToPageIdSet.put(tid,pids);

        /**
         * 如果线程池中有该page
         */
        if(pageIdToPageIdNode.containsKey(pid)){
            Page page  = pageIdToPageIdNode.get(pid).value;
            moveToStart(pageIdToPageIdNode.get(pid));
            return page;
        }

        /**
         * 如果没有该page，并且线程池满了
         */
        if(pageIdToPageIdNode.size() >= this.realNumPage){
            //每次移除队尾页面
            evictPage();
        }

        Page newPage = null;
        try {
            newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        }catch (Exception e){
            e.printStackTrace();
        }
        pageIdNode newNode = new pageIdNode(pid, newPage);
        pageIdToPageIdNode.put(pid,newNode);
        this.lock.lock();
        try{
            //添加新页面，直接add
            addToStart(newNode);
            this.realNumPage++;
        }catch (Exception e){
            System.out.println(e);
        }finally {
            this.lock.unlock();
        }
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

        this.transactionLockManager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.transactionLockManager.holdsLock(tid,p);
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
        Set<PageId> pids = null;
        try {
            pids = this.transactionIdToPageIdSet.get(tid);
            if(commit){
                /**
                 * 对tid涉及的页进行flushPage操作
                 *
                 */
                flushPages(tid,true);
            }else{
                /**
                 * 对页码进行回滚操作
                 */
                for(PageId pid : pids){
                    if(pageIdToPageIdNode.get(pid) != null && tid.equals(pageIdToPageIdNode.get(pid).value.isDirty())){
                        //说明是该事务造成了该页成为脏页
                        Page restorePage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                        //this.pageIdToPageIdNode.put(pid, new pageIdNode(pid, restorePage)); /// ============>断链了，应该修改对应pageIdNode里面的内容
                        pageIdNode needModifyNode = this.pageIdToPageIdNode.get(pid);
                        needModifyNode.key = pid;
                        needModifyNode.value = restorePage;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //释放掉锁
            if(pids == null) return;
            for(PageId pageId : pids){
                this.transactionLockManager.unlock(tid,pageId);
            }
        }
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
            //moveToStart(pageIdToPageIdNode.get(dirtiedPage.getId()));
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
            //moveToStart(pageIdToPageIdNode.get(dirtiedPage.getId()));
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
        if(!this.pageIdToPageIdNode.containsKey(pid)) return;
        this.deleteNode(this.pageIdToPageIdNode.get(pid));   //删除掉该节点
        this.pageIdToPageIdNode.remove(pid);
    }

    private synchronized  void flushPage(PageId pid) throws IOException {
        flushPage(pid,false);
    }


    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid,boolean needSetBeforeImage) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.pageIdToPageIdNode.get(pid).value;
        // append an update record to the log, with
        // a before-image and after-image.
        if(page == null) return;
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
            //如果该页不为空并且该页是脏的
            //应当调用对应文件的write方法来写进去新的page
            DbFile heapFile =  Database.getCatalog().getDatabaseFile(pid.getTableId());
            heapFile.writePage(page);
            page.markDirty(false,null);

            //刷页之后setBeforeImage
            if(needSetBeforeImage)
                page.setBeforeImage();
        }
    }

    public synchronized  void flushPages(TransactionId tid) throws IOException {
        flushPages(tid,false);
    }


    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid,boolean needSetBeforeImage) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pageIds = this.transactionIdToPageIdSet.get(tid);
        if(pageIds == null || pageIds.size() == 0){
            //说明该事务在缓冲池中没有放页码
            return;
        }
        for(PageId pageId : pageIds){
            flushPage(pageId,needSetBeforeImage);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //移除掉最后一页
        /**
         * 不驱逐脏页===》 如果全是脏页，则抛出异常
         */
        pageIdNode pageIdNode = end;
        while(pageIdNode.pre != start){
            pageIdNode = pageIdNode.pre;
            Page page = pageIdNode.value;
            if(page.isDirty() != null) continue;
            //否则说明是干净页，可以驱逐出去
            this.discardPage(page.getId());
            this.realNumPage--;
            return;
        }
        throw new DbException("all page is dirty, so evictPage fail");
    }

}

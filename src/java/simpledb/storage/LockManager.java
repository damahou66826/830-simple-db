package simpledb.storage;

import com.sun.xml.internal.ws.config.management.policy.ManagementPolicyValidator;
import simpledb.common.Permissions;
import simpledb.common.Type;
import simpledb.transaction.TransactionId;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

    public enum LockType implements Serializable{
        ReadLock(),
        WriteLock()
    }

    private volatile Map<TransactionId, HashMap<PageId,Lock>> tidToPageLockMap;

    private volatile Map<PageId,LockType> pageIdToLockType;

    private volatile Map<PageId,TransactionId> pageIdToTrans;


    public LockManager(){
        tidToPageLockMap = new ConcurrentHashMap<>();
        pageIdToLockType = new ConcurrentHashMap<>();
        pageIdToTrans = new ConcurrentHashMap<>();
    }

    private volatile static LockManager lockManager;

    public static LockManager getInstance(){
        if(lockManager == null){
            synchronized (LockManager.class){
                if(lockManager == null){
                    lockManager = new LockManager();
                }
            }
        }
        return lockManager;
    }

    public synchronized void putLock(TransactionId tid,PageId pageId,Lock lock){
        LockType lockType = lock.getClass().equals(ReentrantReadWriteLock.ReadLock.class)?LockType.ReadLock:LockType.WriteLock;
        if(!tidToPageLockMap.containsKey(tid)){
            tidToPageLockMap.put(tid,new HashMap<PageId,Lock>());
        }
        Map<PageId,Lock> map = tidToPageLockMap.get(tid);
        map.put(pageId,lock);
        pageIdToTrans.put(pageId,tid);
        pageIdToLockType.put(pageId,lockType);
    }

    public synchronized Lock getLock(TransactionId tid,PageId pageId){
        if(!tidToPageLockMap.containsKey(tid)) return null;
        return tidToPageLockMap.get(tid).getOrDefault(pageId,null);
    }


    public synchronized void removeLock(TransactionId tid,PageId pageId){
        //需要去解锁
        Map<PageId,Lock> map = tidToPageLockMap.get(tid);
        if(map.containsKey(pageId)){
            Lock lock = map.get(pageId);
            lock.unlock();
        }
        map.remove(pageId);
        pageIdToTrans.remove(pageId);
        pageIdToLockType.remove(pageId);
    }

    public synchronized LockType getLockType(PageId pid){
        return pageIdToLockType.getOrDefault(pid,null);
    }

    public synchronized void removeAllLock(TransactionId tid){
        HashMap<PageId,Lock> map = this.tidToPageLockMap.get(tid);
        for(Map.Entry<PageId,Lock> entry : map.entrySet()){
            removeLock(tid, entry.getKey());
        }
    }

    public synchronized void upgradeLock(TransactionId tid,PageId pageId){
        /**
         * 可能会唤醒其他线程
         */
        removeLock(tid,pageId);
    }

}

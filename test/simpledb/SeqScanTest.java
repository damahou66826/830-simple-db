package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.execution.SeqScan;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;
import static org.junit.Assert.*;
import org.junit.Assert;

public class SeqScanTest {

    @Test
    /**
     * 测试是否能够get到符合要求格式的，拼接好字符串的desc
     */
    public void testGetTupleDesc(){
        TupleDesc td1, td2,td3;

        td1 = Utility.getTupleDesc(3, "td1");
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(1, td1));
        td2 = Utility.getTupleDesc(4, "td2");
        Database.getCatalog().addTable(new TestUtil.SkeletonFile(2, td2));

        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid,1,"ThisIsASimpleTest");
        td3 = seqScan.getTupleDesc();
        System.out.println(td3.getFieldName(1));
        System.out.println("ThisIsASimpleTest." + td1.getFieldName(1));
        assertEquals(td3.getFieldName(1),"ThisIsASimpleTest." + td1.getFieldName(1));
    }
}

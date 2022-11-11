package simpledb;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.transaction.TransactionId;
import simpledb.storage.*;

import java.io.*;

public class Lab1Test {
    public static void main(String[] args) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data.dat"), descriptor);
        /**
         * 测试一下tuple
         */
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            //System.out.println("===");
            while (f.hasNext()) {
                //System.out.println("===");
                Tuple tup = f.next();
                System.out.println(tup.toString());
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
}

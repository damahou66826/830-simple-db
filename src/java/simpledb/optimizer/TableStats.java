package simpledb.optimizer;

import net.sf.antcontrib.logic.Throw;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final DbFile trackedFile;

    private int ioCostPerpage = IOCOSTPERPAGE;

    private int tupleNum;

    private DbFileIterator tupleIterator;

    private IntHistogram[] intHistograms;

    private StringHistogram[] stringHistograms;

    private TupleDesc tupleDesc;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.trackedFile = Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerpage = ioCostPerPage;
        this.tupleNum = 0;

        // 1. generate histograms
        tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        this.intHistograms = new IntHistogram[tupleDesc.numFields()];
        this.stringHistograms = new StringHistogram[tupleDesc.numFields()];
        this.tupleIterator =  this.trackedFile.iterator(new TransactionId());

        // 1.1 find min max of each field
        int[] min = new int[this.tupleDesc.numFields()];
        int[] max = new int[this.tupleDesc.numFields()];
        Arrays.fill(min,Integer.MAX_VALUE);
        Arrays.fill(max,Integer.MIN_VALUE);

        try {
            this.tupleIterator.open();
            while (tupleIterator.hasNext()){
                Tuple next = tupleIterator.next();
                for (int i = 0; i < this.tupleDesc.numFields(); i++) {
                    if(tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) continue;
                    IntField field = (IntField) next.getField(i);

                    min[i] = Math.min(min[i],field.getValue());
                    max[i] = Math.max(max[i],field.getValue());
                }
                this.tupleNum++;
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        // 1.2 construct histograms
        for (int i = 0; i < tupleDesc.numFields(); ++i) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                this.intHistograms[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
            } else {
                this.stringHistograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        // 1.3 build histograms
        try {
            this.tupleIterator.rewind();
            while (this.tupleIterator.hasNext()){
                Tuple next = this.tupleIterator.next();
                for (int i = 0; i < this.tupleDesc.numFields() ;i++){
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)){
                        this.intHistograms[i].addValue(((IntField)next.getField(i)).getValue());
                    }else{
                        this.stringHistograms[i].addValue(((StringField)next.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        long theFileSize = this.trackedFile.getTupleDesc().getSize() * tupleNum;
        int pageNum = (int) Math.ceil(theFileSize * 1.0 / BufferPool.getPageSize());
        return pageNum * this.ioCostPerpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.tupleNum * 1.0 * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //自己的方法
//        Type theTableToFieldType = this.trackedFile.getTupleDesc().getFieldType(field);
//        if(!theTableToFieldType.equals(constant.getType())){
//            //比较的内容不合格，抛出异常
//            try {
//                throw new Exception();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        if(constant.getType().equals(Type.INT_TYPE)){
//            //intType要遍历两遍  呜呜呜
//            List<Integer> lst = new ArrayList<>();
//            int curMax = Integer.MIN_VALUE;
//            int curMin = Integer.MAX_VALUE;
//            try {
//                this.tupleIterator.rewind();
//                while (this.tupleIterator.hasNext()) {
//                    Tuple tuple = this.tupleIterator.next();
//                    int targetNum = ((IntField)tuple.getField(field)).getValue();
//                    curMax = Math.max(curMax,targetNum);
//                    curMin = Math.min(curMin,targetNum);
//                    lst.add(targetNum);
//                }
//            } catch(DbException e){
//                e.printStackTrace();
//            } catch(TransactionAbortedException e){
//                e.printStackTrace();
//            }
//            IntHistogram intHistogram = new IntHistogram(curMax - curMin + 1,curMin,curMax);
//            for(int nu : lst){
//                intHistogram.addValue(nu);
//            }
//            return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
//        }else{
//            StringHistogram stringHistogram = new StringHistogram(10);
//            try {
//                this.tupleIterator.rewind();
//                while (this.tupleIterator.hasNext()) {
//                    Tuple tuple = this.tupleIterator.next();
//                    String targetString = ((StringField)tuple.getField(field)).getValue();
//                    stringHistogram.addValue(targetString);
//                }
//            } catch(DbException e){
//                e.printStackTrace();
//            } catch(TransactionAbortedException e){
//                e.printStackTrace();
//            }
//            return stringHistogram.estimateSelectivity(op,((StringField)constant).getValue());
//        }


        //60w的方法
        if(tupleDesc.getFieldType(field).equals(Type.INT_TYPE)){
            return intHistograms[field].estimateSelectivity(op,((IntField)constant).getValue());
        }
        return stringHistograms[field].estimateSelectivity(op,((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleNum;
    }

}

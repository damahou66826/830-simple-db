package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.utils.twoTuple;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final Type gbFieldType;

    private final int aField;

    private final Op op;

    private Map<Integer, twoTuple> map;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.aField = afield;
        this.gbfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.op = what;
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int tempValue;
        if(this.gbFieldType == null){
            tempValue = -10000;
        }else{
            tempValue = tup.getField(gbfield).hashCode();
        }

        if(map.isEmpty() || !map.containsKey(tempValue)){
            map.put(tempValue,new twoTuple(this.gbFieldType));
            if(this.gbFieldType != null){
                map.get(tempValue).setField(0,tup.getField(this.gbfield));
            }
        }

        twoTuple temp = map.get(tempValue);
        temp.setmin(tup.getField(aField).hashCode());
        temp.setmax(tup.getField(aField).hashCode());
        temp.setSum(tup.getField(aField).hashCode());
        temp.setCount();
        temp.setavg();
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        List<Tuple> target;
        //对于上面所计算的内容进行一个整合操作
        if(this.gbFieldType == null){
            twoTuple targetTuple = map.get(-10000);
            switch (this.op){
                case MIN:
                {
                    targetTuple.setField(0,new IntField(targetTuple.getMin()));
                    break;
                }
                case MAX:
                {
                    targetTuple.setField(0,new IntField(targetTuple.getMax()));
                    break;
                }
                case SUM:
                {
                    targetTuple.setField(0,new IntField(targetTuple.getSum()));
                    break;
                }
                case COUNT:
                {
                    targetTuple.setField(0,new IntField(targetTuple.getCount()));
                    break;
                }
                case AVG:
                {
                    targetTuple.setField(0,new IntField(targetTuple.getAvg()));
                    break;
                }
            }
            target = new ArrayList<>(Arrays.asList(targetTuple));
        }else{
            target = new ArrayList<>();
            for(Map.Entry<Integer,twoTuple> item : map.entrySet()){
                twoTuple targetTuple = item.getValue();
                switch (this.op){
                    case MIN:
                    {
                        targetTuple.setField(1,new IntField(targetTuple.getMin()));
                        break;
                    }
                    case MAX:
                    {
                        targetTuple.setField(1,new IntField(targetTuple.getMax()));
                        break;
                    }
                    case SUM:
                    {
                        targetTuple.setField(1,new IntField(targetTuple.getSum()));
                        break;
                    }
                    case COUNT:
                    {
                        targetTuple.setField(1,new IntField(targetTuple.getCount()));
                        break;
                    }
                    case AVG:
                    {
                        targetTuple.setField(1,new IntField(targetTuple.getAvg()));
                        break;
                    }
                }
                target.add(targetTuple);
            }
        }
        /**
         * iterable与iterator
         * 所有的集合类都实现了iterable
         *
         *
         * 自己写内部类  再去实现这个iterator
         */
        return new TupleIterator(target.get(0).getTupleDesc(), target);
    }

}

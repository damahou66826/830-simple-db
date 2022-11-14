package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;
import simpledb.utils.twoTuple;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final Type gbFieldType;

    private final int aField;

    private final Op op;

    private Map<Integer, twoTuple> map;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) throw new IllegalArgumentException();
        this.aField = afield;
        this.gbfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.op = what;
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
        temp.setCount();
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> target;
        //对于上面所计算的内容进行一个整合操作
        if(this.gbFieldType == null) {
            twoTuple targetTuple = map.get(-10000);
            targetTuple.setField(0, new IntField(targetTuple.getCount()));
            target = new ArrayList<>(Arrays.asList(targetTuple));
        }else{
            target = new ArrayList<>();
            for(Map.Entry<Integer,twoTuple> item : map.entrySet()){
                twoTuple targetTuple = item.getValue();
                targetTuple.setField(1,new IntField(targetTuple.getCount()));
                target.add(targetTuple);
            }
        }
        return new TupleIterator(target.get(0).getTupleDesc(),target);
    }

}

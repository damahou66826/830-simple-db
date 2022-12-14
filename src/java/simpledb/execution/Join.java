package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final OpIterator child1;

    private final OpIterator child2;

    private final JoinPredicate p;

    private OpIterator[] childs;

    /**
     * 维护两个Tuple
     * 为了全局性做笛卡尔积
     */

    private Tuple child1Tuple;

    private Tuple child2Tuple;


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.child1 = child1;
        this.child2 = child2;
        this.p = p;
        childs = null;

        child1Tuple = null;
        child2Tuple = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(this.child1.getTupleDesc(),this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child1Tuple = null;
        this.child2.close();
        this.child2Tuple = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        //this.child1.rewind();
        //this.child2.rewind();
        open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(child1 == null || child2 == null) return null;
        if(!this.child2.hasNext()){

            if(!this.child1.hasNext()) return null;
            else{
                this.child1Tuple = this.child1.next();
                this.child2.rewind();
                return fetchNext();
            }
        }else{
            //照顾一下最开始时候child1为null
            if(this.child1Tuple == null && this.child1.hasNext()){
                this.child1Tuple = this.child1.next();
            }
            //不论如何  往下面遍历
            this.child2Tuple = this.child2.next();
            //说明既有 this.child1Tuple  又有 this.child2Tuple
            if(p.filter(this.child1Tuple,this.child2Tuple)){
                //是要找的东西
                Tuple target = new Tuple(this.getTupleDesc());
                int index = 0;
                int temp = 0;
                while(temp < this.child1Tuple.getTupleDesc().numFields()){
                    target.setField(index,this.child1Tuple.getField(temp));
                    index++;
                    temp++;
                }
                temp = 0;
                while(temp < this.child2Tuple.getTupleDesc().numFields()) {
                    target.setField(index, this.child2Tuple.getField(temp));
                    index++;
                    temp++;
                }
                return target;
            }
            return fetchNext();  //找下一位
        }
        //return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return childs;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.childs = children;
    }

}

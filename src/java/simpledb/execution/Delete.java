package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;

    private OpIterator opIterator;


    private final TupleDesc tupleDesc;

    private boolean isCalled;



    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.transactionId = t;

        this.opIterator = child;

        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});

        this.isCalled = false;


    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.opIterator.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.isCalled) return null;
        this.isCalled = true;
        int count = 0;
        while(this.opIterator.hasNext()){
            Tuple needDeleteTuple = this.opIterator.next();
            try {
                Database.getBufferPool().deleteTuple(this.transactionId,needDeleteTuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0,new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.opIterator = children[0];
    }

}

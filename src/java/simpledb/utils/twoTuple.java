package simpledb.utils;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

public class twoTuple extends Tuple {

    private int count;

    private int sum;

    private int min;

    public int getCount() {
        return count;
    }

    public int getSum() {
        return sum;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public int getAvg() {
        return avg;
    }

    private int max;

    private int avg;

    public twoTuple(Type gbfieldtupe){
        super(gbfieldtupe == null ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtupe,Type.INT_TYPE}) );
        this.sum = 0;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
        this.count = 0;
        this.avg = 0;
    }

    public void setSum(int num){
        this.sum += num;
    }

    public void setCount(){
        this.count++;
    }

    public void setmax(int num){
        this.max = Math.max(this.max,num);
    }

    public void setmin(int num){
        this.min = Math.min(this.min,num);
    }

    public void setavg(){
        if(this.count != 0)
            this.avg = this.sum / this.count;
    }


}

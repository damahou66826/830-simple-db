package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;

    private final int min;

    private final int max;

    private int bucketsWidth;   //====>为什么要整数?

    private volatile int nTups;

    private int[] intHistogramList;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;

        this.min = min;

        this.max = max;
        this.bucketsWidth = (int) Math.ceil((max - min + 1) / (buckets * 1.0));
        intHistogramList = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketsId = (int) ((v - min) / (this.bucketsWidth * 1.0));
        this.intHistogramList[bucketsId]++;
        this.nTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here

        if(op.equals(Predicate.Op.EQUALS)){
            if(judgeTooLarget(v) || judgeTooSmall(v)) return 0;
            int num = this.intHistogramList[(int) ((v - min) / (this.bucketsWidth * 1.0))];
            return ((1/(this.bucketsWidth * 1.0)) * num) / nTups;
        }

        if(op.equals(Predicate.Op.LESS_THAN)){
            /**
             * 考虑边界情况
             */
            if(judgeTooLarget(v)) return 1.0;
            if(judgeTooSmall(v)) return 0;

            int index = 0;
            int num = 0;
            while(bucketMaxValue(index) < v){
                num += this.intHistogramList[index];
                index++;
            }
            int targetBucketsNum = bucketHeight(index);  //来详细计算对应桶中的tuple个数
            num += ((v - bucketMinValue(index)) / (this.bucketsWidth * 1.0)) * targetBucketsNum;
            return (double)num / this.nTups;
        }

        if(op.equals(Predicate.Op.GREATER_THAN)){
            /**
             * 考虑边界情况
             */
            if(judgeTooLarget(v)) return 0;
            if(judgeTooSmall(v)) return 1.0;

            int index = this.buckets - 1;
            int num = 0;
            while(bucketMinValue(index) > v){
                num += this.intHistogramList[index];
                index--;
            }
            int targetBucketsNum = bucketHeight(index);  //来详细计算对应桶中的tuple个数
            num += ((bucketMaxValue(index) - v) / (this.bucketsWidth * 1.0)) * targetBucketsNum;
            return (double) num / this.nTups;
        }

        if(op.equals(Predicate.Op.NOT_EQUALS)){
            return 1 - estimateSelectivity(Predicate.Op.EQUALS,v);
        }

        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
            double temp =estimateSelectivity(Predicate.Op.GREATER_THAN,v);
            return 1 - temp;
        }

        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }
        return -1.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < this.intHistogramList.length; ++i) {
            sb.append(bucketMinValue(i)).append("-").append(bucketMaxValue(i)).append(": ").append(this.intHistogramList[i]);

            if (i < this.intHistogramList.length- 1)
                sb.append(", ");
        }

        return sb.toString();
    }


    public boolean judgeTooLarget(int v){
        return v > this.max;
    }

    public boolean judgeTooSmall(int v){
        return v < this.min;
    }

    private int bucketIndex(int v) {
        if (v < this.min) {
            return - 1;
        } else if (v > this.max) {
            return this.intHistogramList.length;
        } else {
            return (int) Math.floor((v - this.min) / (this.bucketsWidth * 1.0));
        }
    }

    private int bucketMinValue(int index) {
        return (int) (this.min + index * this.bucketsWidth);
    }

    private int bucketMaxValue(int index) {
        return (int) (this.min + ((index + 1) * this.bucketsWidth) - 1);
    }

    private int bucketHeight(int index) {
        return (index >= 0 && index < this.intHistogramList.length) ? this.intHistogramList[index] : 0;
    }
}

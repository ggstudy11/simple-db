package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    private final int min;
    private final int max;
    private final int gap;
    private final int[] buckets;
    private int total;
    public final static double ALL = 1.0;
    public final static double NULL = 0.0;
    public IntHistogram(int buckets, int min, int max) {
        this.min = min;
        this.max = max;
        this.gap = Math.max(1, (max - min) / buckets);
        this.buckets = new int[buckets];
        total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // 如果在边界都算右边的桶
        int index = Math.min((v - min) / gap, buckets.length - 1);
        buckets[index]++;
        total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v > max || v < min) {
            if (op.equals(Predicate.Op.EQUALS)) return NULL;
            if (op.equals(Predicate.Op.NOT_EQUALS)) return ALL;
            boolean less = op.equals(Predicate.Op.LESS_THAN_OR_EQ) || op.equals(Predicate.Op.LESS_THAN);
            boolean greater = op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ);
            if (v > max){
                if (less) return ALL;
                if (greater) return NULL;
            } else {
                if (less) return NULL;
                if (greater) return ALL;
            }
        }
        int index = Math.min((v - min) / gap, buckets.length - 1);
        double ratio = buckets[index] * 1.0 / (gap * total);
        if (op.equals(Predicate.Op.EQUALS)) {
            return ratio;
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - ratio;
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            double contributeRatio = ratio * ((index + 1) * gap + min - v);
            for (int i = index + 1; i < buckets.length; ++i) {
                contributeRatio += buckets[i] * 1.0 / total;
            }
            return contributeRatio;
        }
        if (op.equals(Predicate.Op.LESS_THAN)) {
            double contributeRatio = ratio * (v - (index * gap + min));
            for (int i = index - 1; i >= 0; --i) {
                contributeRatio += buckets[i] * 1.0 / total;
            }
            return contributeRatio;
        }
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            double contributeRatio = ratio * ((index + 1) * gap + min - v);
            for (int i = index + 1; i < buckets.length; ++i) {
                contributeRatio += buckets[i] * 1.0 / total;
            }
            return contributeRatio;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            double contributeRatio = ratio * (v - (index * gap + min));
            for (int i = index - 1; i >= 0; --i) {
                contributeRatio += buckets[i] * 1.0 / total;
            }
            return contributeRatio;
        }
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // 计算bucket的平均选中率
        double avg = 0;
        for (int bucket : buckets) {
            avg += bucket * 1.0 / total;
        }
        return avg / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return null;
    }
}

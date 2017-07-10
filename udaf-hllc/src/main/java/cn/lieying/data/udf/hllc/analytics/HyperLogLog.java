package cn.lieying.data.udf.hllc.analytics;

import cn.lieying.data.udf.hllc.exception.CardinalityMergeException;
import cn.lieying.data.udf.hllc.hash.MurmurHash;

import java.util.Arrays;

/**
 * Created by jarvis on 2017/3/27~
 */
public class HyperLogLog implements ICardinality {

    /**
     * Gamma function computed using Mathematica
     * AccountingForm[
     * N[With[{m = 2^Range[0, 31]},
     * m (Gamma[-1/m]*(1 - 2^(1/m))/Log[2])^-m], 14]]
     */

    private final int k;
    private int m; // length of M (not cardinality))
    private byte[] M; // set of data
    private int Rsum = 0;

    private final double alphaMM;

    public HyperLogLog(int k) { // constructor input integer
        this.k = k;
        // In this case, 1 is being multiplied by 2^k
        this.m = 1 << k; // x << y is equal to x*(2^y) arithmetic left shift
        // x >> y is equal to x/(2^y) arithmetic right shift
        this.M = new byte[m]; // generates byte array of size m
        this.alphaMM = getAlphaMM(k, m);
    }

    public HyperLogLog(byte[] M) { // constructor given input byte array
        this.M = M;
        this.m = M.length;
        this.k = Integer.numberOfTrailingZeros(m);
        // assert is essentially an if statement of (m == (1 << k))
        // checks if # of buckets is a power of 2 and greater than 0
        assert (m == (1 << k)) : "Invalid array size: M.length must be a power of 2";
        // for each loop for bytes in M
        for (byte b : M) {
            Rsum += b;
        }

        this.alphaMM = getAlphaMM(k, m);
    }

    //retrieve byte array
    public byte[] getBytes() {
        return M;
    }

    //retrieve length of M
    public int sizeof() {
        return m;
    }

    //program does not yet support longs
    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }

    // adds bytes
    public boolean offerHashed(int hashedInt) {
        boolean modified = false;
        // unsigned right shift, regardless of sign, shifts all bits to right and replaces rest with 0s
        int j = hashedInt >>> (Integer.SIZE - k);
        // byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        byte r = (byte)rho(hashedInt, k);
        if (M[j] < r) {
            Rsum += r - M[j];
            M[j] = r;
            modified = true;
        }
        return modified;
    }

    public boolean offer(Object o) {
        int x = MurmurHash.hash(o);
        return offerHashed(x);
    }

    /**
     * Computes the position of the first set bit of the last Integer.SIZE-k bits
     *
     * @return Integer.SIZE-k if the last k bits are all zero
     */
    protected static int rho(int x, int k) {
        return Integer.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1;
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of HyperLogLog of the same size)
     */
    public ICardinality merge(ICardinality... estimators) {

        if (estimators == null) {
            return new HyperLogLog(M);
        }

        byte[] mergedBytes = Arrays.copyOf(this.M, this.M.length);
        for (ICardinality estimator : estimators) {
            if (!(this.getClass().isInstance(estimator))) {
                throw new LogLogMergeException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof()) {
                throw new LogLogMergeException("Cannot merge estimators of different sizes");
            }
            HyperLogLog ll = (HyperLogLog) estimator;
            for (int i = 0; i < mergedBytes.length; ++i) {
                mergedBytes[i] = (byte) Math.max(mergedBytes[i], ll.M[i]);
            }
        }

        return new HyperLogLog(mergedBytes);
    }

    /**
     * @return this estimator bytes
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of HyperLogLog of the same size)
     */
    public byte[] getM() {
        return this.M;
    }

    /**
     * @return this estimator bucket size
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of HyperLogLog of the same size)
     */
    public int getm() {
        return this.m;
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static HyperLogLog mergeEstimators(HyperLogLog... estimators) throws LogLogMergeException {
        if (estimators == null || estimators.length == 0) {
            return null;
        }
        return (HyperLogLog) estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }

    /**
     * exception
     */
    @SuppressWarnings("serial")
    public static class LogLogMergeException extends CardinalityMergeException {

        public LogLogMergeException(String message) {
            super(message);
        }
    }

    private static double getAlphaMM(final int p, final int m) {
        // See the paper.
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

    public long cardinality() {
        double registerSum = 0;
        int count = this.m;
        double zeros = 0;
        for (byte pMax : this.M) {
            registerSum += 1.0 / (1 << pMax);
            if (pMax == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            return Math.round(linearCounting(count, zeros));
        } else {
            return Math.round(estimate);
        }
    }
    private double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }
}

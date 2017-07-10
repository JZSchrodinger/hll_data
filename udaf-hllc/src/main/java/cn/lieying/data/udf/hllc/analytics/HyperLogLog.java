package cn.lieying.data.udf.hllc.analytics;

import cn.lieying.data.udf.hllc.exception.CardinalityMergeException;
import cn.lieying.data.udf.hllc.hash.MurmurHash;

import java.util.Arrays;

/**
 * Created by rore256 on 7/6/17
 */
public class HyperLogLog implements ICardinality {

    private final int k; // split for buckets between test & PMax
    private int m; // length of M (not cardinality))
    private byte[] M; // set of data

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
        byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        if (M[j] < r) {
            M[j] = r;
            modified = true;
        }
        return modified;
    }

    // takes in a line, passes to hash to hash it
    public boolean offer(Object o) {
        int x = MurmurHash.hash(o);
        return offerHashed(x);
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
     * exception
     */
    @SuppressWarnings("serial")
    public static class LogLogMergeException extends CardinalityMergeException {

        public LogLogMergeException(String message) {
            super(message);
        }
    }

    // retuns the number with the lowest error rate
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
    // Linear Counting method
    private double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }
}

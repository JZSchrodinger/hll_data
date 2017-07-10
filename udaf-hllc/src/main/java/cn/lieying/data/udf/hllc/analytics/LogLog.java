package cn.lieying.data.udf.hllc.analytics;

import cn.lieying.data.udf.hllc.exception.CardinalityMergeException;
import cn.lieying.data.udf.hllc.hash.MurmurHash;

import java.util.Arrays;
import java.lang.Integer;

/**
 * Created by rore256 on 7/6/2017.
 */
abstract class LogLog implements ICardinality{

    private final int k;
    private int m; // length of M (not cardinality))
    private byte[] M; // set of data
    private int Rsum = 0;

    public LogLog(int k) { // constructor input integer
        this.k = k;
        // In this case, 1 is being multiplied by 2^k
        this.m = 1 << k; // x << y is equal to x*(2^y) arithmetic left shift
        // x >> y is equal to x/(2^y) arithmetic right shift
        this.M = new byte[m]; // generates byte array of size m
    }

    //retrieve byte array
    public byte[] getBytes() {
        return M;
    }

    //retrieve length of M
    public int sizeof() {
        return m;
    }

    //our data has no pure numbers so the
    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }

    // this is the hashed String/byte[]
    public boolean offerHashed(int hashedInt) {
        boolean modified = false;
        // unsigned right shift, regardless of sign, shifts all bits to right and replaces rest with 0s
        // shifts hashedInt is being divided by the number of bits after leading 0
        int j = hashedInt >>> (Integer.SIZE - k);
        // r is number after leading 0s
        byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        // checks which one is larger
        if (M[j] < r) {
            Rsum += r - M[j];
            M[j] = r;
            modified = true;
        }
        return modified;
    }

    /**
     * Takes in a String of the characters in the data and hashes it using the MurmurHash method
     * Ex. a0000029ffc202
     * @return return isn't important but just validates that the program ran successfully
     **/
    public boolean offer(Object o) {
        // Hashes the String
        int x = MurmurHash.hash(o);
        System.out.println("hash is " + x);
        return offerHashed(x);
    }

}

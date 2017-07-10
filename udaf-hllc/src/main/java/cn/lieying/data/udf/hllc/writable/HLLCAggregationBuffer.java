package cn.lieying.data.udf.hllc.writable;

import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;


/**
 * Created by Administrator on 2017/5/25.
 */
public class HLLCAggregationBuffer implements GenericUDAFEvaluator.AggregationBuffer {

    private HyperLogLog hll;

    public HLLCAggregationBuffer(int bucketNum) {
        hll = new HyperLogLog(bucketNum);
    }

    public HLLCAggregationBuffer(byte[] data) {
        hll = new HyperLogLog(data);
    }

    public HyperLogLog getHll() {
        return hll;
    }

    public void setHll(HyperLogLog hll) {
        this.hll = hll;
    }
}

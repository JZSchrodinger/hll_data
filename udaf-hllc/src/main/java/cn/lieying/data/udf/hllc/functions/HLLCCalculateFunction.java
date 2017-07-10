package cn.lieying.data.udf.hllc.functions;

import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import cn.lieying.data.udf.hllc.config.CommonConfig;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Administrator on 2017/6/6.
 */
public class HLLCCalculateFunction extends UDF {

    public Long evaluate(String text) {

        if (text == null) {
            return 0l;
        }
        if (text.getBytes().length != (1 << CommonConfig.BUCKET_COUNT)) {
            return 0l;
        }
        HyperLogLog hyperLogLog = new HyperLogLog(text.getBytes());
        return hyperLogLog.cardinality();
    }
}

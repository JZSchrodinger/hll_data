package cn.lieying.data.udf.hllc.functions;

import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import cn.lieying.data.udf.hllc.config.CommonConfig;
import cn.lieying.data.udf.hllc.writable.HLLCAggregationBuffer;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * Created by Administrator on 2017/5/25.
 */
public class HLLCBucketFunction extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        // Type-checking goes here!
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }
        return new HLLCBucketFunctionEvaluator();
    }

    public static class HLLCBucketFunctionEvaluator extends GenericUDAFEvaluator {


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new HLLCAggregationBuffer(CommonConfig.BUCKET_COUNT);
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {

        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            HLLCAggregationBuffer result = (HLLCAggregationBuffer) aggregationBuffer;
            for (Object item : objects) {
                Text custom_id = (Text) item;
                if (custom_id != null && custom_id.getLength() > 0) {
                    try {
                        HyperLogLog hyperLogLog = new HyperLogLog(CommonConfig.BUCKET_COUNT);
                        hyperLogLog.offer(custom_id.getBytes());
                        result.setHll((HyperLogLog) result.getHll().merge(hyperLogLog));
                    } catch (HyperLogLog.LogLogMergeException e) {
                        throw new HiveException(e.getMessage());
                    }
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            HLLCAggregationBuffer result = (HLLCAggregationBuffer) aggregationBuffer;
            HLLCAggregationBuffer p = (HLLCAggregationBuffer) o;

            try {
                result.setHll((HyperLogLog) result.getHll().merge(p.getHll()));
            } catch (HyperLogLog.LogLogMergeException e) {
                throw new HiveException(e.getMessage());
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            HLLCAggregationBuffer buf = (HLLCAggregationBuffer) aggregationBuffer;
            return new Text(buf.getHll().getBytes());
        }
    }

}

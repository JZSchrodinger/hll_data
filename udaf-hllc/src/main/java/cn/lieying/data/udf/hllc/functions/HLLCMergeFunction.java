package cn.lieying.data.udf.hllc.functions;

import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import cn.lieying.data.udf.hllc.config.CommonConfig;
import cn.lieying.data.udf.hllc.writable.HLLCAggregationBuffer;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Administrator on 2017/5/25.
 */
public class HLLCMergeFunction extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        // Type-checking goes here!
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }
        return new HLLCMergeFunctionEvaluator();
    }

    public static class HLLCMergeFunctionEvaluator extends GenericUDAFEvaluator {

        protected PrimitiveObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            //init input
            inputOI = (PrimitiveObjectInspector) parameters[0];
            switch (m) {
                case PARTIAL1:
                case PARTIAL2:
                    //init output
                    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                default:
                    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new HLLCAggregationBuffer(CommonConfig.BUCKET_COUNT);
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            HLLCAggregationBuffer result = (HLLCAggregationBuffer) aggregationBuffer;
            result.setHll(new HyperLogLog(CommonConfig.BUCKET_COUNT));
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert parameters.length == 1;
            this.merge(aggregationBuffer, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            HLLCAggregationBuffer result = (HLLCAggregationBuffer) aggregationBuffer;
            return new Text(result.getHll().getBytes());
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial != null) {
                HLLCAggregationBuffer result = (HLLCAggregationBuffer) aggregationBuffer;
                String bucket = PrimitiveObjectInspectorUtils.getString(partial, inputOI);
                byte[] data = bucket.getBytes();
                HLLCAggregationBuffer buffer = new HLLCAggregationBuffer(data);
                result.setHll((HyperLogLog) result.getHll().merge(buffer.getHll()));
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            HLLCAggregationBuffer buf = (HLLCAggregationBuffer) aggregationBuffer;
            return new LongWritable(buf.getHll().cardinality());
        }
    }
}

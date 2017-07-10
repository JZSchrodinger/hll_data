import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Administrator on 2017/7/6.
 */
public class TestDemo {

    @Test
    public void test1() throws IOException {
        HyperLogLog testLogLog = new HyperLogLog(15);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\thatq\\Downloads\\data"));
        String line;
        int size = 10000;
        while ((line = bufferedReader.readLine()) != null) {
            String[] array = line.split("\\|");
            if (array.length >= 1) {
                String imei = array[0];
                testLogLog.offer(imei);
            }
            size--;
            if (size == 0) {
                break;
            }
        }
        long uv = testLogLog.cardinality();
        System.out.println("There are " + uv + " unique values!");
    }

}
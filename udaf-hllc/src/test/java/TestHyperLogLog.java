import cn.lieying.data.udf.hllc.analytics.HyperLogLog;
import org.junit.Test;

import java.io.*;

/**
 * Created by Administrator on 2017/5/25.
 */
public class TestHyperLogLog {

    @Test
    public void test1() {
        HyperLogLog hyperLogLog = new HyperLogLog(15);
        hyperLogLog.offer("isenhome".getBytes());
    }

    @Test
    public void test2() {
        System.out.println((byte) Integer.numberOfLeadingZeros(1));
        System.out.println(Integer.numberOfLeadingZeros(2));
        System.out.println(Integer.numberOfLeadingZeros(3));
        System.out.println(Integer.numberOfLeadingZeros(4));
        System.out.println(Integer.numberOfLeadingZeros(5));
    }

    @Test
    public void test3() {
        try {
            DataInputStream dataInputStream = new DataInputStream(new FileInputStream("C:\\Users\\Administrator\\Desktop\\a.data"));
//            long counter = dataInputStream.readLong();
//            System.out.println("counter:" + counter);
            byte[] bytes = new byte[1 << 15];
            dataInputStream.readFully(bytes);
            HyperLogLog hyperLogLog = new HyperLogLog(bytes);
            hyperLogLog.cardinality();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test4() {
        System.out.println(1 << 15);
    }

    @Test
    public void test5() {
        System.out.println(hashCode("Booking全球酒店预订超过110万间住宿遍布全球".toCharArray()));
        System.out.println("Booking全球酒店预订超过110万间住宿遍布全球".hashCode());
        System.out.println("Booking全球酒店预订超过110万间住宿遍布全球lieying".hashCode());

    }

    public int hashCode(char[] value) {
        int h = 0;
        if (h == 0 && value.length > 0) {
            char val[] = value;

            for (int i = 0; i < value.length; i++) {
                h = 31 * h + val[i];
            }
        }
        return h;
    }
}

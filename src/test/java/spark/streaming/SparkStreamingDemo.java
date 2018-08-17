package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingDemo {

    private  JavaStreamingContext jsc =  null;

    @Before
    public void bef(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingDemo");
        jsc = new JavaStreamingContext(conf, Durations.seconds(10));
    }

    /**
     * tcp
     *  输入
     * @throws InterruptedException
     */
    @Test
    public void demo1() throws InterruptedException {
        JavaReceiverInputDStream<String> lins = jsc.socketTextStream("192.168.86.129", 9999);
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = lins.flatMap(c -> {
            return Arrays.asList(c.split(" ")).iterator();
        }).mapToPair(c -> new Tuple2<>(c, 1)).reduceByKey((a, b) -> a + b);
        stringIntegerJavaPairDStream.print();
        jsc.start();
        jsc.awaitTermination();
    }

    /**
     * 文件输入
     */
    @Test
    public void demo2() throws InterruptedException {
        JavaDStream<String> stringJavaDStream = jsc.textFileStream("testfile/streaming");
        stringJavaDStream.flatMap(c->Arrays.asList(c.split(" ")).iterator()).
                mapToPair(c->new Tuple2<>(c,1)).reduceByKey((a,b)->a+b).print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }


}

package spark.sparkrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by MyPC on 2018/8/2.
 */
public class SparkLocalFileRdd {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        SparkConf conf = new SparkConf().setAppName("SparkLocalFileRdd").setMaster("local");
        sc = new JavaSparkContext(conf);
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     * 使用本地文件创建rdd 统计字母个数
     */
    @Test
    public void demo1(){
        JavaRDD<String> stringJavaRDD = sc.textFile("D:/test2.log");
        JavaRDD<Integer> lin = stringJavaRDD.map(c -> c.replaceAll(" ","").length());
        Integer num = lin.reduce((a, b) -> a + b);
        System.out.println(num);
    }
    /**
     * 使用本地文件创建rdd 统计单词数量
     */
    @Test
    public void demo2(){
        JavaRDD<String> stringJavaRDD = sc.textFile("D:/test2.log");
        JavaRDD<Integer> lin = stringJavaRDD.map(c -> Arrays.asList(c.split(" ")).size());
        Integer num = lin.reduce((a, b) -> a + b);
        System.out.println(num);
    }
    /**
     * 使用本地文件创建rdd 统计单词数量
     */
    @Test
    public void demo3(){
        JavaRDD<String> stringJavaRDD = sc.textFile("D:/test2.log");
        JavaRDD<List<String>> map = stringJavaRDD.map(c -> Arrays.asList(c.split(" ")));
        JavaRDD<String> stringJavaRDD1 = map.flatMap(c -> c.iterator());
        JavaPairRDD<String, Integer> javaPairRDD = stringJavaRDD1.mapToPair(c -> new Tuple2(c, 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = javaPairRDD.reduceByKey((a, b) -> a + b);
        stringIntegerJavaPairRDD.foreach(c->{
            System.out.println(c);
        });
    }
    /**
     * 使用本地文件创建rdd 统计行数
     */
    @Test
    public void demo4(){
        JavaRDD<String> stringJavaRDD = sc.textFile("D:/test2.log");
        JavaRDD<Integer> map = stringJavaRDD.map(c -> 1);
        Integer reduce = map.reduce((a, b) -> a + b);
        System.out.println(reduce);

    }
    /**
     * 使用本地文件创建rdd 统计相同行出现的次数
     */
    @Test
    public void demo5(){
        JavaRDD<String> lins = sc.textFile("D:/test2.log");
        JavaPairRDD<String, Integer> linTuple = lins.mapToPair(c -> new Tuple2(c, 1));
        JavaPairRDD<String, Integer> result = linTuple.reduceByKey((a, c) -> a + c);
        result.foreach(c->{
            System.out.println(c._1+":"+c._2);
        });
    }
}

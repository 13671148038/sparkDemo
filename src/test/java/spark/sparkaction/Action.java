package spark.sparkaction;

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
import java.util.Map;

/**
 * Created by MyPC on 2018/8/3.
 */
public class Action {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        SparkConf confi = new SparkConf().setMaster("local").setAppName("Action");
        sc = new JavaSparkContext(confi);
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     * reduce 操作
     * 操作的本质就是聚合   就是累加
     */
    @Test
    public void demo1(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list);
        Integer sum = javaRDD.reduce((a, b) -> a + b);
        System.out.println(sum);
    }

    /**
     * collect 操作
     *collect 操作会将集群中的的rdd传输到本地.如果rdd中的数据量过大会出现内存溢出,
     * 这个collect方法一般不常用,
     * 因此推荐使用foreach方法进行结果处理
     */
    @Test
    public void demo2(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> map = sc.parallelize(list).map(a -> a * 2);
        List<Integer> collect = map.collect();
        for (int i =0;i<collect.size();i++){
            System.out.println(collect.get(i));
        }
    }

    /**
     * count 操作
     * 统计rdd 中的个数
     */
    @Test
    public void demo3(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        long count = parallelize.count();
        System.out.println(count);
    }

    /**
     * take操作
     *获取指定前几条的额数据
     */
    @Test
    public void demo4(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        List<Integer> take = parallelize.take(3);
        take.forEach(a->{
            System.out.println(a);
        });
    }

    /**
     *saveAsTextFile操作
     * 将文件保存到指定的路径中
     */
    @Test
    public void demo5(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Integer> integerJavaRDD = parallelize.map(a -> a * 2);
        integerJavaRDD.saveAsTextFile("sparksave");
    }

    /**
     *countByKey操作
     * 根据键key统计元素的个数
     */
    @Test
    public void demo6(){
        List<Tuple2<String, String>> tuple2s = Arrays.asList(
                new Tuple2<String, String>("class1", "lisi"),
                new Tuple2<String, String>("class2", "sadc"),
                new Tuple2<String, String>("class1", "sdcvr"),
                new Tuple2<String, String>("class2", "oihjo"),
                new Tuple2<String, String>("class", "sadcwec")
        );
        JavaPairRDD<String, String> stringStringJavaPairRDD = sc.parallelizePairs(tuple2s);
        Map<String, Long> stringLongMap = stringStringJavaPairRDD.countByKey();
        stringLongMap.forEach((a,b)->{
            System.out.println(a+":"+b);
        });
    }
}

package spark.sparktransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by MyPC on 2018/8/2.
 */
public class Transformation {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        SparkConf conf = new SparkConf().setAppName("Transformation").setMaster("local");
        sc = new JavaSparkContext(conf);
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     * map使用
     */
    @Test
    public void demo1(){
        List<Integer>  list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> nums = sc.parallelize(list);
        JavaRDD<Integer> map = nums.map(c -> c * 2);
        List<Integer> collect = map.collect();
        collect.forEach(c->System.out.println(c));
    }
    /**
     * filter使用
     */
    @Test
    public void demo2(){
        List<Integer>  list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> nums = sc.parallelize(list);
        List<Integer> collect = nums.filter(c -> c > 5).collect();
        collect.forEach(c->{
            System.out.println(c);
        });
    }
    /**
     * flatMam的使用
     */
    @Test
    public void demo4(){
        List<String> collect = sc.textFile("D:/test2.log").
                flatMap(c -> Arrays.asList(c.split(" ")).iterator()).
                collect();
    }
    /**
     * flatMam的使用
     */
    @Test
    public void demo5(){
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<String, Integer>("class1", 57),
                new Tuple2<String, Integer>("class2", 55),
                new Tuple2<String, Integer>("class1", 58),
                new Tuple2<String, Integer>("class2", 50)
        );
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = sc.parallelizePairs(tuple2s).groupByKey();
        stringIterableJavaPairRDD.foreach(c->{
            String banji = c._1();
            System.out.println(banji);
            Iterator<Integer> iterator = c._2().iterator();
            iterator.forEachRemaining(f->System.out.println(f));
        });
    }
    /**
     * reduceByKey的使用
     */
    @Test
    public void demo6(){
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<String, Integer>("class1", 57),
                new Tuple2<String, Integer>("class2", 55),
                new Tuple2<String, Integer>("class1", 58),
                new Tuple2<String, Integer>("class2", 50)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(tuple2s).reduceByKey((a, b) -> a + b);
        stringIntegerJavaPairRDD.foreach(c->{
            System.out.println(c._1+":"+c._2());
        });
    }
    /**
     * reduceByKey的使用 通过key进行排序
     */
    @Test
    public void demo7(){
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<String, Integer>("class1", 57),
                new Tuple2<String, Integer>("class2", 55),
                new Tuple2<String, Integer>("class1", 58),
                new Tuple2<String, Integer>("class2", 50)
        );
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = sc.parallelizePairs(tuple2s).mapToPair(c -> new Tuple2<Integer, String>(c._2, c._1));
        integerStringJavaPairRDD.sortByKey().foreach(c->System.out.println(c));
    }
    /**
     * join的使用
     */
    @Test
    public void demo8(){
        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(new Tuple2<Integer, String>(1, "lisi"),
                new Tuple2<Integer, String>(2, "wanglu"),
                new Tuple2<Integer, String>(3, "zhaoliu"),
                new Tuple2<Integer, String>(4, "xiaohang"),
                new Tuple2<Integer, String>(2, "nidayede")
        );
        List<Tuple2<Integer, Integer>> tuple2s2 = Arrays.asList(new Tuple2<Integer, Integer>(1, 39),
                new Tuple2<Integer, Integer>(2, 54),
                new Tuple2<Integer, Integer>(3, 100),
                new Tuple2<Integer, Integer>(4, 58),
                new Tuple2<Integer, Integer>(1, 897)
        );
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = sc.parallelizePairs(tuple2s);
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = sc.parallelizePairs(tuple2s2);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = integerStringJavaPairRDD.join(integerIntegerJavaPairRDD);
        join.foreach(c->{
            Integer key = c._1;
            String s = c._2()._1();
            Integer integer1 = c._2()._2();
            System.out.println(key+" "+s+" "+integer1);
        });
    }
    /**
     * cogroup的使用
     */
    @Test
    public void demo9(){
        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(new Tuple2<Integer, String>(1, "lisi"),
                new Tuple2<Integer, String>(2, "wanglu"),
                new Tuple2<Integer, String>(3, "zhaoliu"),
                new Tuple2<Integer, String>(4, "xiaohang"),
                new Tuple2<Integer, String>(1, "nidayede")
        );
        List<Tuple2<Integer, Integer>> tuple2s2 = Arrays.asList(new Tuple2<Integer, Integer>(1, 39),
                new Tuple2<Integer, Integer>(2, 54),
                new Tuple2<Integer, Integer>(3, 100),
                new Tuple2<Integer, Integer>(4, 58),
                new Tuple2<Integer, Integer>(2, 897)
        );
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = sc.parallelizePairs(tuple2s);
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = sc.parallelizePairs(tuple2s2);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = integerStringJavaPairRDD.cogroup(integerIntegerJavaPairRDD);
        cogroup.foreach(c->{
            Integer integer = c._1();
            Tuple2<Iterable<String>, Iterable<Integer>> iterableIterableTuple2 = c._2();
            System.out.println(integer+""+iterableIterableTuple2);
        });
    }

}

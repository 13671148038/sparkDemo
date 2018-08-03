package spark.baseanli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * spark 高级编程值二次排序
 */
public class SparlTwiceSort {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        SparkConf conf =new SparkConf().setMaster("local").setAppName("SparlTwiceSort");
        sc= new JavaSparkContext(conf);
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     * 二次排序
     * 原理 先将第二列进行排序,然后再将第一列进行排序
     */
    @Test
    public void demo1(){
        List<Tuple2<Integer, Integer>> tuple2s = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 3),
                new Tuple2<Integer, Integer>(3, 6),
                new Tuple2<Integer, Integer>(1, 43),
                new Tuple2<Integer, Integer>(4, 32),
                new Tuple2<Integer, Integer>(3, 23),
                new Tuple2<Integer, Integer>(4, 9)
        );
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = sc.parallelizePairs(tuple2s);
        //先将第一列与第二列进行位置调换后进行排序,这样第二列的书序是从小到大的
        integerIntegerJavaPairRDD.mapToPair(c->new Tuple2<Integer,Integer>(c._2,c._1)).sortByKey()
                //然后再进行位置调换,在进行排序
                .mapToPair(c->new Tuple2<Integer,Integer>(c._2,c._1)).sortByKey().
                foreach(c->{
                    System.out.println(c._1+":"+c._2);
                });
    }
}

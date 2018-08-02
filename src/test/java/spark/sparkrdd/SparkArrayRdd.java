package spark.sparkrdd;

/**
 * Created by MyPC on 2018/8/2.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 通过集合创建RDD
 */
public class SparkArrayRdd {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        SparkConf conf = new SparkConf().setAppName("SparkArrayRdd").setMaster("local");
        sc = new JavaSparkContext(conf);
    }
    @After
    public void end(){
        sc.close();
    }
    /**
     * 1到10累加
     */
    @Test
    public void demo1(){
        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        Integer sum = parallelize.reduce((a, b) -> a + b);
        System.out.println(sum);
    }
}

package spark.persist;

/**
 * Created by MyPC on 2018/8/3.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * spark数据持久化
 */
public class SparkPersist {
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
     *cache()  是在第一次action后会将lins进行持久化,再一次就行action操作的时候
     * 会从内存中或者磁盘中进行读取会节省很多时间
     * **注意 sc.textFile("d:/test2.log").cache(); 只能这么写
     *
     * 这样分两行来写的话,数据会丢失也会报错,不会起作用
     * JavaRDD<String> lins = sc.textFile("d:/test2.log");
     * lins..cache()
     *
     */
    @Test
    public void demo(){
        JavaRDD<String> lins = sc.textFile("d:/test2.log").cache();
        long startTime = System.currentTimeMillis();
        long count = lins.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime);
        long startTime2 = System.currentTimeMillis();
        long count2 = lins.count();
        System.out.println(count2);
        long endTime2 = System.currentTimeMillis();
        System.out.println(endTime2-startTime2);
    }
}

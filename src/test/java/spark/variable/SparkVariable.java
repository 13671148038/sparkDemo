package spark.variable;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by MyPC on 2018/8/3.
 * spark的共享变量和累加变量
 */
public class SparkVariable {
    private JavaSparkContext sc = null;
    @Before
    public void bef(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        SparkConf confi = new SparkConf().setMaster("local").setAppName("SparkVariable");
        sc = new JavaSparkContext(confi);
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     *
     * 共享,广播变量 broadcast
     * 会给每一个节点共性一份变量,如果不适用共享变量将会给每个节点的每个task分配一份变量,这将会是很多分变量.
     */
    @Test
    public void deom1(){
        Integer f = 3;
        //创建共享变量
        Broadcast<Integer> broadcast = sc.broadcast(f);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        //使用共享变量调用器value()就可以获取共享变量
        parallelize.map(c -> c * broadcast.value()).
                foreach(c->{
                    System.out.println(c);
                });
    }

    /**
     * 累加变量accumulator
     *
     */
    @Test
    public void demo2(){
        Integer f= 0;
        //创建累加变量leijia
        Accumulator<Integer> leijia = sc.accumulator(f);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        parallelize.foreach(c->{
            //使用其add() 进行累加,在这里面是task进程 是不可以获取leijia的值的就是不能调用他的value()方法
            leijia.add(c);
            //Integer value = leijia.value(); 这一行代码不能执行 以为这是在task中  只能累加操作,错误为:Caused by: java.lang.UnsupportedOperationException: Can't read accumulator value in task
        });
        //在driver程序中调用value() 查看其值
        Integer value = leijia.value();
        System.out.println(value);
    }
}

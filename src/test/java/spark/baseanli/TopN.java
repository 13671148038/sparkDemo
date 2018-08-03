package spark.baseanli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 去除文本中数字最大的前三个
 */
public class TopN {
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
     * 取除最大的前三个数
     */
    @Test
    public void demo1(){
        JavaRDD<String> lins = sc.textFile("d:/topn.txt");
        lins.mapToPair(c->new Tuple2<Integer,Integer>(Integer.valueOf(c),1)).
                sortByKey(false).map(c->c._1).take(3).
                forEach(c->{
                    System.out.println(c);
                });
    }
    /**
     * 取除每个班级成绩最大的前三个
     */
    @Test
    public void demo2(){
        JavaRDD<String> lins = sc.textFile("d:/topn2.txt");
        /*lins.mapToPair(c->{
            String[] s = c.split(" ");
            return  new Tuple2<String,Integer>( s[0],Integer.valueOf( s[1]));
        }).
        groupByKey().mapToPair(c->{
            List<Integer> top3 = new ArrayList<>();
            Iterable<Integer> integers = c._2;
            ArrayList<Integer> sdc = (ArrayList<Integer>)integers;
            Collections.sort(sdc);
            int size = sdc.size();
            int start = 0;
            if(size>=3){
                size=3;
                start-=3;
            }
            for (int i=size-1;i>start;i--){
                top3.add(sdc.get(i));
            }
            return new Tuple2<String,List<Integer>>(c._1,top3);
        }).foreach(c->
                System.out.println(c)
                );*/
        ;
    }
}

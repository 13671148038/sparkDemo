package spark.baseanli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

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
     * 取出最大的前三个数
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
     * 取出每个班级成绩最大的前三个
     */
    @Test
    public void demo2(){
        JavaRDD<String> lins = sc.textFile("testfile/topn2.txt");
        lins.mapToPair(c->{
            String[] s = c.split(" ");
            return new Tuple2<String,Integer>(s[0],Integer.valueOf(s[1]));
        }).
        groupByKey().mapToPair(c->{
            Integer[] top3 = new Integer[3];
            Iterator<Integer> iterator = c._2().iterator();
            while (iterator.hasNext()){
                Integer next = iterator.next();
               if(top3[2]==null){
                   for (int i=0;i<3;i++){
                       if(top3[i]==null){
                           top3[i]=next;
                           break;
                       }
                   }
               }else{
                   Integer min = top3[0];
                   Integer index = 0;
                   for (int j =0;j<3;j++){
                       if(min>top3[j]){
                           min=top3[j];
                           index=j;
                       }
                   if(next>min){
                       top3[index]=next;
                   }
                   }
               }
            }
            Arrays.sort(top3);
            return new Tuple2<String,Integer[]>(c._1,top3);
        }).foreach(c-> {
                System.out.print(c._1);
                System.out.print(c._2[0]+" ");
                System.out.print(c._2[1]+" ");
                System.out.println(c._2[2]); }
        );
    }
}

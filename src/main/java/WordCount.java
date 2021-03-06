import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by MyPC on 2018/8/1.
 */
public class WordCount {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("wordCount");
//        SparkConf sparkConf = new SparkConf().setAppName("wordCount");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("hdfs://192.168.86.129:9000/user/MyPC/logs/test2.log", 2);
        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.flatMap(c -> {
            List<String> strings = Arrays.asList(c.split(" "));
            return strings.iterator();
        });
        JavaPairRDD<String, Integer> counts = stringJavaRDD1.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = counts.reduceByKey((a, b) -> a + b);
        stringIntegerJavaPairRDD.foreach(c->System.out.println(c._1+" 是:"+c._2()+" 次"));
    }
}

package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by MyPC on 2018/8/1.
 */
public class SparkSql {

    private SparkSession sparkSession = null;
    @Before
    public void setHadoop(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        sparkSession = SparkSession.builder()
                .appName("SparkSqlDemo")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:/Users/MyPC/Desktop/spark-warehouse")
                .getOrCreate();
    }
    //json
    @Test
    public void demo1(){
        Dataset<Row> json = sparkSession.read().json("E:/people.json");
        json.show();
        json.printSchema();
        json.select("name").show();
        json.groupBy("age").count().show();

        json.createOrReplaceTempView("people");
        Dataset<Row> sql = sparkSession.sql("select * from people");
    }
    //dataBase
    @Test
    public void dataBase(){
        String url = "jdbc:mysql://localhost:3306/excel?characterEncoding=utf-8&serverTimezone=UTC";
        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","root");
        Dataset<Row> user = sparkSession.read().jdbc(url, "user", properties);
        user.show();
        user.createOrReplaceTempView("user");
        Dataset<Row> sql = sparkSession.sql("select userName from user");
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        sql.show();
    }
}

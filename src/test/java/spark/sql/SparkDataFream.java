package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * sparkSQL测试
 */
public class SparkDataFream {
    private SparkSession sparkSession = null;
    @Before
    public void bef(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        sparkSession = SparkSession.builder()
                .appName("SparkSqlDemo")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:/Users/zhush/Desktop/spark-warehouse")
                .getOrCreate();
    }

    /**
     * 基本的石sql 语句
     * 读取sql文件
     */
    @Test
    public void demo1(){
        Dataset<Row> people = sparkSession.read().json("testfile/json/people.json");
        //打印
        people.show();
        //打印元数据
        people.printSchema();
        //插叙某列所有数据
        people.select("name").show();
        //查询某几列的所有数据
        people.select("name","age").show();
        //根据木某一列的值进行过滤
        people.filter(new Column("age").gt(40)).show();
        //根据某一列进行分组然后聚合
        people.groupBy("age").count().show();
    }

    /**
     *
     */
    @Test
    public void demo2(){
        Dataset<Row> parquet = sparkSession.read().parquet("testfile/parquet/users.parquet");
        //创建中间表
        parquet.createOrReplaceTempView("parquet");
        //查询name
        Dataset<Row> select_name_from_parquet = sparkSession.sql("select name from parquet");
        //转换成list
        List<String> collect = select_name_from_parquet.javaRDD().map(c -> c.getString(0)).collect();
        collect.forEach(c->{
            System.out.println(c);
        });
    }

}

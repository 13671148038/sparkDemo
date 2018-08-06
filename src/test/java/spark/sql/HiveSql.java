package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

/**
 * hivesql
 */
public class HiveSql {
    private SparkSession sparkSession = null;
    @Before
    public void bef(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
          sparkSession= SparkSession.builder().
                appName("HiveSql").
                master("local").enableHiveSupport().
                config("spark.sql.warehouse.dir","hdfs://192.168.86.129:9000/user/hive/warehouse").
                getOrCreate();
    }
    @Test
    public void demo1(){
        Dataset<Row> show_tables = sparkSession.sql("create table if not exists goodstudent (id STRING,name STRING,age INT)");
        sparkSession.sql("insert into goodstudent values ('dewdwc','李四',34),('dewdsdwc','王五',43),('dewdwsdcc','赵六',44)");
        Dataset<Row> select_name_from_goodstudent = sparkSession.sql("select name from goodstudent");
    }
}

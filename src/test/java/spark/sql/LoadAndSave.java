package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

/**
 * Spark的load  parquet文件类型
 * 和save 保存类型
 */
public class LoadAndSave {
    private SparkSession sparkSession = null;
    @Before
    public void bef(){
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        sparkSession = SparkSession.builder()
                .appName("LoadAndSave")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:/Users/MyPC/Desktop/spark-warehouse")
                .getOrCreate();
    }
    @Test
    public void demo1(){
        Dataset<Row> load = sparkSession.read().load("testfile/parquet/users.parquet");
        load.select("name", "favorite_color").write().save("testfile/json/jsh");
    }

}

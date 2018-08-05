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

    /**
     * 使用load方法加载数据,默认文件类型是parquet类型 也可以使用format() 进行制定文件类型
     *
     * 调用save方法的时候 也可以使用format()进行制定的数据类型进行保存
     */
    @Test
    public void demo1(){
        Dataset<Row> load = sparkSession.read().load("testfile/parquet/users.parquet");
        load.createOrReplaceTempView("users");
        Dataset<Row> select_name_from_users = sparkSession.sql("select name from users");
        select_name_from_users.write().save("testfile/parquet/save");
    }

}

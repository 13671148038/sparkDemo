package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.junit.Before;
import org.junit.Test;

/**
 * 自动分区
 */
public class AutoPartition {
    private SparkSession sparkSession  = null;
    @Before
    public void bef(){
        sparkSession= SparkSession.builder().
                appName("AutoPartition")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:/Users/zhush/Desktop/spark-warehouse")
                .getOrCreate();
    }
    @Test
    public void demo1(){
        Dataset<Row> json = sparkSession.read().parquet("testfile/json/gender=modal/");
        json.show();
    }
}









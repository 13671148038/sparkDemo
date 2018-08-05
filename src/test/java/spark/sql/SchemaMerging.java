package spark.sql;

import bean.Student;
import bean.Student2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 将两个parquet文件进行合并
 */
public class SchemaMerging {
    private SparkSession sparkSession = null;
    @Before
    public void bef(){
        sparkSession=SparkSession.builder()
                .master("local")
                .appName("ScjemaMerging")
                .config("spark.sql.warehouse.dir", "C:/Users/zhush/Desktop/spark-warehouse")
                .getOrCreate();
    }

    /**
     * 同时合并两个parquet文件
     */
    @Test
    public void demo1(){
    //创建第一个parquet文件
        List<Student> list1 = new ArrayList<>();
        list1.add(new Student("李四","男",20));
        list1.add(new Student("秀琴","女",19));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(list1, Student.class);
        dataFrame.write().save("testfile/parquet/student/key=1");
        //创建第二个parquet文件
        List<Student2> list2 = new ArrayList<>();
        list2.add(new Student2("五班","李老师"));
        list2.add(new Student2("六班","朱老师"));
        Dataset<Row> dataFrame1 = sparkSession.createDataFrame(list2, Student2.class);
        dataFrame1.write().save("testfile/parquet/student/key=2");
        //同时读取两个文件并且合并成一个dataset
        Dataset<Row> mergeSchema = sparkSession.read().option("mergeSchema", true).parquet("testfile/parquet/student/");
        mergeSchema.show();
    }
}

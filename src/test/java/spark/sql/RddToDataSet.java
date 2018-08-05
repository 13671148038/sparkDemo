package spark.sql;

import bean.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 将RDD转换成dataSet
 */
public class RddToDataSet {
    private JavaSparkContext sc= null;
    private SparkSession sparkSession = null;
    @Before
    public void bef(){
        SparkConf conf= new SparkConf().setMaster("local").setAppName("RddToDataSet");
        sc = new JavaSparkContext(conf);
        sparkSession = SparkSession.builder().appName("SparkSqlDemo")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:/Users/MyPC/Desktop/spark-warehouse")
                .getOrCreate();
    }
    @After
    public void end(){
        sc.close();
    }

    /**
     * Rdd 一反射的方式转成DataSet<Row>
     */
    @Test
    public void demo1(){
        JavaRDD<String> lins = sc.textFile("testfile/text/student.txt");
        JavaRDD<Student> students = lins.map(c -> {
            String[] s = c.split(" ");
            Student student = new Student();
            student.setName(s[0]);
            student.setAge(Integer.valueOf(s[1]));
            student.setSex(s[2]);
            return student;
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(students, Student.class);
        dataFrame.show();
        dataFrame.createOrReplaceTempView("students");
        Dataset<Row> sql = sparkSession.sql("select * from students where age > 40");
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        JavaRDD<Student> studentRdd = rowJavaRDD.map(c -> {
            Student student = new Student();
            student.setAge(c.getLong(0));
            student.setName(c.getString(1));
            student.setSex(c.getString(2));
            return student;
        });
        studentRdd.foreach(c->{
            System.out.println(c);
        });
    }

    /**
     * 动态方法将RDD装换成DataSet<Wow>
     */
    @Test
    public void demo2(){

        JavaRDD<String> lins = sc.textFile("testfile/text/student.txt");
        //javaRdd的泛型必须是Row
        JavaRDD<Row> map = lins.map(c -> {
            String[] s = c.split(" ");
            Row row = RowFactory.create(s[0], Long.valueOf(s[1]), s[2]);
            return row;
        });
        map.foreach(c->{
            System.out.println(c);
        });
        //创建元数据
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age", DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("sex", DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(map, structType);
        dataFrame.createOrReplaceTempView("students");
        sparkSession.sql("select * from students where age >40").show();
    }
}

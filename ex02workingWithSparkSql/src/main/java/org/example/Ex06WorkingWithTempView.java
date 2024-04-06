package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * this program show how we can use tempview and use spark-sql on top of it.
 */
public class Ex06WorkingWithTempView {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();


        Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/students.csv");


        dataset.createOrReplaceTempView("my_students_view");
        Dataset<Row> results = spark.sql("select * from my_students_view where subject='French'");
        results.show();


        //we can also do selective selection of columns as shown below
        spark.sql("select score,year from my_students_view where subject='French'")
                .show();

        //similarly we can fire othe queries like
        spark.sql("select max(score) from my_students_view where subject='French'")
                .show();

        //finding average
        spark.sql("select avg(score) from my_students_view where subject='French'")
                .show();

        //finding distinct
        spark.sql("select distinct(year) from my_students_view where subject='French'")
                .show();

        //using order by
        spark.sql("select distinct(year) from my_students_view where subject='French' order by year desc")
                .show();

        spark.close();
    }
}

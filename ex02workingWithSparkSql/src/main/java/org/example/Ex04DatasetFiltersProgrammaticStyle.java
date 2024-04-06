package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * this program shows how to we can achieve the previous program output using a programatic filter on the dataset.
 * in contrast int the previous program we have written the filter in more sql style.
 */
public class Ex04DatasetFiltersProgrammaticStyle {

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



//        //combining multiple filter conditions
//        Dataset<Row> modernArtResultForStudentsPost2000 = dataset.filter("subject = 'Modern Art' AND year>=2007" );
//        modernArtResultForStudentsPost2000.show();


        Dataset<Row> modernArtResultForStudentsPost2007 = dataset.filter((FilterFunction<Row>) row ->
                (row.getAs("subject").equals("Modern Art")
                && Integer.parseInt(row.getAs("year"))>=2007)
        );
        modernArtResultForStudentsPost2007.show();

        spark.close();
    }
}

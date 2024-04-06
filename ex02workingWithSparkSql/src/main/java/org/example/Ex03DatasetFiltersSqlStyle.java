package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * this program is explore how to filter data from dataset.
 */
public class Ex03DatasetFiltersSqlStyle {

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


        //just like RDDS the datasets are also immutable
        Dataset<Row> modernArtResult = dataset.filter("subject = 'Modern Art'");
        modernArtResult.show();


        //combining multiple filter conditions
        Dataset<Row> modernArtResultForStudentsPost2000 = dataset.filter("subject = 'Modern Art' AND year>=2007" );
        modernArtResultForStudentsPost2000.show();


        spark.close();
    }
}

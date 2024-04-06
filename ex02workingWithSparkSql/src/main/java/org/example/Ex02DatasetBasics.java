package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * this program is explore dataset basics.
 */
public class Ex02DatasetBasics {

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


        dataset.show(); //this is to print the data of the dataset

        long count = dataset.count();//getting dataset count
        System.out.println("there are "+ count + " records");


        //exploring dataset
        Row firstRow = dataset.first();
        //it is to be noted since source is a csv file so the return type is just objct which needs to be casted
        //here we are reading column values using index
        String subject = firstRow.get(2).toString() ;
        System.out.println(subject);

        //reading column values using column header
        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("year value for the first record is :"+ year);





        spark.close();
    }
}

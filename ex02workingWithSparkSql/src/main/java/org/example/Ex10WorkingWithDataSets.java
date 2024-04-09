package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * working with datetime formatting. here we will understand this with a usecase
 * where the requirement is to get the count of records group by log_level and month
 */
public class Ex10WorkingWithDataSets {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();



        Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/biglog.csv");

        //selecting only required columns
        Dataset<Row> dataset2 = dataset.select("level");
        dataset2.show(100);

        //to select multiple columns
        Dataset<Row> dataset3 = dataset.select("level","datetime");
        dataset3.show();


        //to perform some operation while fetching we have to use selectExpr i.e. select expression
        Dataset<Row> dataset4 = dataset.selectExpr("level","date_format(datetime,'MMM') as month");
        dataset4.show();


        //there are also handy java functions that are provided by spark and in can be used as shown below. note that we
        //are using the dataset.select here and not dataset.selectExpr. also note that these function from the package
        //org.apache.spark.sql.functions
        Dataset<Row> dataset5 = dataset.select(col("level"),date_format(col("datetime"), "MMMM"));
        dataset5.show();


        //however if we need to use column alias alongth with these functions then that can be done as shown below
        Dataset<Row> dataset6 = dataset.select(col("level").as("loglevel"),date_format(col("datetime"), "MMMM").as("datetime"));
        dataset6.show();


        spark.close();
    }
}

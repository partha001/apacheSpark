package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * working with datetime formatting. here we will understand this with a usecase
 * where the requirement is to get the count of records group by log_level and month
 */
public class Ex11WorkingWithGroupByInDataSets {

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


        Dataset<Row> dataset1 = dataset.select(col("level").as("level")
                ,date_format(col("datetime"), "MMMM").as("month")
                ,date_format(col("datetime"), "M").as("monthnum"));
        dataset1 = dataset1.groupBy(col("level"), col("month"), col("monthnum")).count();
        dataset1 = dataset1.orderBy("monthnum");
        dataset1.show(100); //however it is to be noted months are sorted in lexicographical order and not in naturual number ordering
        //this is because spark is treating the monthnum as string and its being ordered as 1, 10, 11 ,12 , 2 , 3


        //now fixing the sorting problem , we  have to tell spark the datatype of monthnum
        // column so that it can do the order by correctly as shown below
        Dataset<Row> dataset2 = dataset.select(col("level").as("level")
                ,date_format(col("datetime"), "MMMM").as("month")
                ,date_format(col("datetime"), "M").as("monthnum").cast(DataTypes.IntegerType));
        dataset2 = dataset2.groupBy(col("level"), col("month"), col("monthnum")).count();
        dataset2 = dataset2.orderBy("monthnum");
        dataset2.show(100); //however it is to be notedt atht the data is not sorted by month





        spark.close();
    }
}

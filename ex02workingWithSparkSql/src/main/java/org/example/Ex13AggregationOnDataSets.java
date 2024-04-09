package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Ex13AggregationOnDataSets {


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


        //say the requirement is to find max(score) per subject
        //if we simply do max(score) from students group by subject then it will give datatype error
        //     Dataset<Row> dataset2 = dataset.groupBy("subject").max("score");
        //since to spark every column contains string [as we are dealing with csv here] . so we have to explicitly tell spark that score is a number
        //and that can be done as below



        //dataset = dataset.groupBy("subject").max(col("score"));

        Dataset<Row> dataset2  = dataset.groupBy("subject").agg(functions.max(col("score").cast(DataTypes.IntegerType)));
        dataset2.show();

        //note that here are creating the column object separately and then using it in the max()
        Column score = dataset.col("score");
        Dataset<Row> dataset3  = dataset.groupBy("subject").agg(functions.max(score.cast(DataTypes.IntegerType)).alias("max score"));
        dataset3.show();



        //now lets find the max and the min
        Dataset<Row> dataset4  = dataset.groupBy("subject")
                .agg(functions.max(col("score").cast(DataTypes.IntegerType)).alias("max_score"),
                        functions.min(col("score").cast(DataTypes.IntegerType)).alias("min_score"));
        dataset4.show();

    }
}

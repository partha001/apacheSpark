package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * working with datetime formatting. here we will understand this with a usecase
 * where the requirement is to get the count of records group by log_level and month
 */
public class Ex09DateTimeFormatting {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();

//        /** in memory datasetup start **/
//        List<Row> inMemory = new ArrayList<Row>();
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//        inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
//
//        StructField[] fields = new StructField[]{
//                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//        };
//        StructType schema = new StructType(fields);
//        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);//a dataframe can be thought of collection of datarows
//        /** in memory datasetup end**/


        /** to work with the csv file datasource **/
        Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/biglog.csv");
        /** **/



        //approach1: first doing transformation of date into on dataset. then creating a tempView on top of it and then doing groupby to get the result
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results1 = spark.sql("select level, date_format(datetime,'MMMM') as  month from logging_table");
        results1.show();
        //now to getting count on top of the previous dataset. here we are creating a tempView .
        results1.createOrReplaceTempView("logging_table2");
        Dataset<Row> result2 = spark.sql("select level, month, count(1) as count from logging_table2 group by level, month order by level, month");
        result2.show();


        //approach2: doing transformation and group by on directly on the initial tempView in one query
        Dataset<Row> results2 = spark.sql("select level, date_format(datetime,'MMMM') as  month, count(*) as count  from logging_table group by level, month order by level,month");
        results2.show();

        spark.close();
    }
}

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
 *
 */
public class Ex08GroupingAndAggregationInSparkSql {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();

        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);//a dataframe can be thought of collection of datarows

        dataset.createOrReplaceTempView("logging_table");
//        Dataset<Row> results = spark.sql("select level,datatime from logging_table group by level");
//        results.show();

        Dataset<Row> results1 = spark.sql("select level,collect_list(datetime) from logging_table group by level");
        results1.show();
        //apart from collect_list there are lot of other aggregate functions available. in fact spark has more
        //aggregate functions than sql


        //using aggregate functions
        Dataset<Row> results2 = spark.sql("select level,count(datetime) from logging_table group by level");
        results2.show();

        //order by can also be optionally added as shown below
        Dataset<Row> results3 = spark.sql("select level,count(datetime) from logging_table group by level order by level");
        results3.show();

        spark.close();
    }
}

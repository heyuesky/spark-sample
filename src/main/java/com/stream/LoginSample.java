package com.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class LoginSample {

    public static Dataset<Row> groupByFirstLogin(Dataset<Row> dataset) {

        /*
         等价于如下 SQL

         select date, count(user_id) as numbers
             from (
                select min(time) as date from login group by user_id
             )
         group by date

         * */

        return dataset.groupBy("user_id")
                .agg(functions.min("time").alias("date"))
                .groupBy("date")
                .agg(functions.count("user_id").alias("numbers"));
    }
}

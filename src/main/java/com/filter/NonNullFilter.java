package com.filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class NonNullFilter {

    public static Dataset<Row> filter(Dataset<Row> source, String columnName) {
        return source.filter(source.apply(columnName).isNotNull());
    }
}

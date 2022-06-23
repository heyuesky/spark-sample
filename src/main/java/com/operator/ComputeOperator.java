package com.operator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ComputeOperator {

    Dataset<Row> apply(Dataset<Row> source);
}

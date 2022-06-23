package com.operator;

import com.filter.NonNullFilter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;

public class MyFieldFilterOperator implements ComputeOperator {

    private final List<String> columnNames;

    public MyFieldFilterOperator(List<String> columnNames) {
        this.columnNames = columnNames;
    }


    @Override
    public Dataset<Row> apply(Dataset<Row> source) {
        // 这个 operator 是基于 filter 实现的

        // 先过滤 name 对应字段是 null 的
        // length 是字符串长度, 过滤不大于0的
        for (String name : columnNames) {
            source = NonNullFilter.filter(source, name).filter(
                    functions.length(source.col(name)).gt(functions.lit(0))
            );
        }
        return source;
    }
}

package com.udf;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.UUID;

public class UuidGeneratorOperator {

    /**
     * 生成唯一 ip
     * */
    public static UserDefinedFunction get() {
        UserDefinedFunction uuidUdf = functions.udf(
                ()-> UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE, DataTypes.LongType
        );
        System.out.println(uuidUdf);
        return uuidUdf;
    }
}

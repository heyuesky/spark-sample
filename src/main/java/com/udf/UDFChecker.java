package com.udf;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class UDFChecker {

    /**
     * 过滤非法 ip
     * */
    public static UserDefinedFunction filterIllegalIp() {
        return functions.udf(
                (String ip) -> {
                    String[] numbers = ip.split("\\.");
                    if (numbers.length != 4) {
                        return false;
                    }
                    for (String n : numbers) {
                        int num = Integer.parseInt(n);
                        if (num > 255 || num < 0) {
                            return false;
                        }
                    }
                    return true;
                },
                DataTypes.BooleanType
        );
    }
}

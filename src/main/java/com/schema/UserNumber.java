package com.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class UserNumber {
    public static StructType schema() {
        return new StructType()
                .add("user_id", DataTypes.StringType)
                .add("count", DataTypes.StringType);
    }
}

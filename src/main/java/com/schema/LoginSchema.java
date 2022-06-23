package com.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class LoginSchema {
    public static StructType schema() {
        return new StructType()
                .add("id", DataTypes.StringType)
                .add("user_id", DataTypes.StringType)
                .add("time", DataTypes.StringType);
    }
}

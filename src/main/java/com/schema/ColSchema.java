package com.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ColSchema {
    public static StructType number() {
        return new StructType()
                .add("date", DataTypes.StringType)
                .add("small", DataTypes.StringType);
    }

    public static StructType chinese() {
        return new StructType()
                .add("date", DataTypes.StringType)
                .add("big", DataTypes.StringType);
    }
}

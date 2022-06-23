package com.basis;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySpark {
    private final SparkSession spark;

    public MySpark() {
        SparkConf conf = new SparkConf().setAppName("dataset").setMaster("local[*]");
        SparkContext context = new SparkContext(conf);
        context.setLogLevel("WARN");
        this.spark = SparkSession.builder()
                .enableHiveSupport()
                .sparkContext(context)
                .getOrCreate();
    }

    public SparkSession spark() {
        return this.spark;
    }

    public String sqlPrepare(String sql) {
        // 做一些替换, 比如表里面有一些日期之类的
        String sqlRet = sql
                .replace("<%=log_date%>", "20220428")
                ;
        return sqlRet;
    }

    public Dataset<Row> sql(String sql) {
        String sqlStr = sqlPrepare(sql);
        return this.spark().sql(sqlStr);
    }

}

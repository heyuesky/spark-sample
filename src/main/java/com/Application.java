package com;

import com.basis.MySpark;
import com.operator.MyFieldFilterOperator;
import com.schema.ColSchema;
import com.schema.LoginSchema;
import com.schema.UserNumber;
import com.stream.LoginSample;
import com.udf.UuidGeneratorOperator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.lit;

public class Application {

    public static void main(String[] args) {
        MySpark conf = new MySpark();
        String loginData = "src/resources/data/login.dat";

        // TODO 一次只运行一个看效果

        // 1.1
        loadData(conf.spark(), loginData);
        // 1.2
        loadLoginData(conf.spark(), loginData);
        // 1.3
        String devicePath = "src/resources/data/device.dat";
        parseDeviceData(conf.spark(), devicePath);
        // 1.4
        complexSample_sql_addCol_write(conf.spark(), loginData);

        // 2.1
        lineMerge(conf.spark());
        // 2.2
        colsMerge(conf.spark());
        // 2.3
        concatMerger(conf.spark());
    }

    /**
     * 最简单的数据加载的例子
     * */
    public static void loadData(SparkSession spark, String dataPath) {
        // load data
        Dataset<Row> dataset = spark.read().json(dataPath);
        dataset.show();
        dataset.printSchema();
    }

    /**
     * 这个例子演示的是用 spark 写代码的方式 来描述 sql
     * */
    public static void loadLoginData(SparkSession spark, String dataPath) {
        System.out.println("=========== login SQL example");
        Dataset<Row> dataset = spark.read().schema(LoginSchema.schema()).csv(dataPath);
        dataset.show();
        dataset = LoginSample.groupByFirstLogin(dataset);
        dataset.show();
    }

    /**
     * 这个例子演示的是用读取 json
     * 解析 json 的 k,v 数据, 拉平展示出来
     * */
    public static void parseDeviceData(SparkSession spark, String dataPath) {
        // _c0
        Dataset<Row> dataset = spark.read().csv(dataPath);
        dataset.show();
        DataType schema = new MapType(DataTypes.StringType, DataTypes.IntegerType, false);
        //
        Column expand = functions.map_concat(functions.from_json(dataset.apply("_c0"), schema));
        dataset = dataset.select(functions.explode(expand))
                .withColumnRenamed("key", "my_key")
                .withColumnRenamed("value", "my_value");
        dataset.show();

    }

    /**
     * 生成单独的列, 然后拼在一起
     * */
    public static void concatMerger(SparkSession sparkSession) {
        StructType schema1 = new StructType(new StructField[]{
                DataTypes.createStructField("number_1", DataTypes.StringType, false),
        });
        List<Row> dateList1 = new ArrayList<>();
        List<String> records1 = List.of("11");
        for (String rec : records1) {
            dateList1.add(RowFactory.create(rec));
        }
        Dataset<Row> number1 = sparkSession.createDataFrame(dateList1, schema1);


        StructType schema2 = new StructType(new StructField[]{
                DataTypes.createStructField("number_2", DataTypes.StringType, false),
        });
        List<Row> dateList2 = new ArrayList<>();
        List<String> records2 = List.of("22");
        for (String rec : records2) {
            dateList2.add(RowFactory.create(rec));
        }
        Dataset<Row> number2 = sparkSession.createDataFrame(dateList2, schema2);


        StructType schema3 = new StructType(new StructField[]{
                DataTypes.createStructField("number_3", DataTypes.StringType, false),
        });
        List<Row> dateList3 = new ArrayList<>();
        List<String> records3 = List.of("33");
        for (String rec : records3) {
            dateList3.add(RowFactory.create(rec));
        }
        Dataset<Row> number3 = sparkSession.createDataFrame(dateList3, schema3);

        List<Dataset<Row>> numberList = List.of(number1, number2, number3);

        Dataset<Row> result = numberList.stream().reduce(Dataset::join).get();

        result.show();
    }

    public static void lineMerge(SparkSession sparkSession) {
        // 行合并
        Dataset<Row> computeResults = sparkSession.read().schema(UserNumber.schema()).csv("src/resources/data/func/line.dat");
        Dataset<Row> computeResultOther = sparkSession.read().schema(UserNumber.schema()).csv("src/resources/data/func/line2.dat");
        computeResults = computeResults.union(computeResultOther);
        computeResults.show();
    }

    /**
     * 这个是说有多个日期对应的 counter 表
     * 通过 join merge 在一起, 并且每个日期都是有
     *
     * 先生成全部的日志的一个表
     * 然后 join
     * */
    public static void colsMerge(SparkSession sparkSession) {
        // 列合并
        // 先生成时间列表
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("_date", DataTypes.StringType, false),
        });

        List<Row> dateList = new ArrayList<>();
        List<String> records = Arrays.asList("2022-01-12", "2022-01-14", "2022-02-14", "2022-02-15", "2022-03-14");
        for (String rec : records) {
            dateList.add(RowFactory.create(rec));
        }
        Dataset<Row> init = sparkSession.createDataFrame(dateList, schema);
        init.show();

        Dataset<Row> computeResults = sparkSession.read().schema(ColSchema.number()).csv("src/resources/data/func/cols1.dat");
        Dataset<Row> computeResultOther = sparkSession.read().schema(ColSchema.chinese()).csv("src/resources/data/func/cols2.dat");
        // merge
        computeResults = init.join(
                computeResults,
                init.col("_date").equalTo(computeResults.col("date")),
                "fullouter"
        );
        computeResults = computeResults.join(
                computeResultOther,
                computeResults.col("_date").equalTo(computeResultOther.col("date")),
                "fullouter"
        );
        computeResults.show();

        // generate db
        computeResults.createOrReplaceTempView("temp");
        computeResults = sparkSession.sql("select sum(small) as cnt from temp");
        computeResults.show();

    }

    /**
     * 复杂例子:
     *      读取数据, 直接使用 spark sql, 新增列, 写入数据(可以使用 jdbc 写入数据库)
     * */
    public static void complexSample_sql_addCol_write(SparkSession sparkSession, String dataPath) {
        Dataset<Row> computeResults;
        computeResults = sparkSession.read().schema(LoginSchema.schema()).csv(dataPath);

        // pre operator
        // 这个过滤器过滤的内容长度, 一个是过滤 null 一个是过滤 "" 空字符串
        MyFieldFilterOperator complexFilter = new MyFieldFilterOperator(List.of("user_id"));
        computeResults = complexFilter.apply(computeResults);

        Dataset<Row> computeResult2 = computeResults
                .withColumn("uuid", UuidGeneratorOperator.get().apply());

        computeResults = computeResults.join(computeResult2,
                computeResults.col("id").equalTo(computeResult2.col("id")),
                "left_outer");

        computeResults.show();


        // compute
        computeResults.createOrReplaceTempView("login");
        computeResults = sparkSession.sql("select * from login where user_id > 5;");
        computeResults.show();

        // functions.lit      来添加简单类型(string,int,float,long,等)的常量列
        // functions.typedLit 可以添加 List / Seq / Map 类型的常量列
        computeResults = computeResults
                .withColumn("uuid", UuidGeneratorOperator.get().apply())
                .withColumn("is_deleted", lit(true))
        ;
        computeResults.write().mode(SaveMode.Append)
                .option("batchsize", String.valueOf(10))
                .csv("src/resources/data/output");
        computeResults.show();
    }
}

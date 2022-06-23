# spark-api

https://spark.apache.org/docs/latest/quick-start.html

### api

1. 数据加载 csv / json / jdbc
2. 数据转换 stream / flatmap / group by
3. 数据过滤 filter
4. python UDF

### 常见概念与问题

遍地垃圾资料, 随便看看就好

#### 优势, 卖点

并行计算 / DAG / 效率高 / 生态好 / 分布式

#### 算子

map filter groupBy flatMap 等都是算子, 简单的就说就是提供了大量列表处理方法

算子概念上还有区分: 

transformation 和 action

transformation: map filter

action: reduce first

部分算子会引起 Shuffle: reduceByKey groupByKey

#### 什么是 Shuffle

Sort 和 Shuffle 是 MapReduce 上最核心的操作之一

不知道, 

#### spark 的惰性机制

在进行创建、转换，如map方法时，不会立即执行，只有在遇到Action如foreach时，三者才会开始遍历运算

#### RDD 和 DataFrame 和 Dataset 三者间的区别

其实没啥好对比的，不同时期的 API 抽象，用新不用旧，Dataset 是最新的

#### spark 作业流程

不知道, 

#### 处理的问题 架构意义

流计算 + 批处理

构建 lambda 架构的大数据应用, lambda 架构很好理解

就是数据足够多的情况下, 做数据分成

实时层(实时新增数据, 比如一天内) + 批处理层(存量数据)

服务层做一些业务批处理视图

#### 竞品

Flink、Spark、Hadoop、Storm

Hadoop 主要解决存储和 MapReduce计算 两个问题, 但是抽象层次低, 只具备批处理计算的能力

所以在业务上为了方便使用, 一般会用 Pig or Tez

spark 开拓了新思路, 抽象了一个分布式数据对象 RDD, 且各种设计上性能更好

flink 在很多设计上做了改进, 比如基于事件的 steaming 流, 毫秒级的延迟等, 但是生态上目前还是 spark 更好一些

[flink >= spark > hadoop](hadoop-spark-flink.md '')
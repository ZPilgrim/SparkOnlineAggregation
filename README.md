# SparkOnlineAggregation
Online Aggregation Library for spark

## How to compile code
### Maven
```
mvn clean compile
```

## How to run code
### Maven
```
mvn exec:java -Dexec.mainClass="com.client.SparkOnlineAggregationClient"
```

## Package
```
mvn assembly:assembly
在target目录会生成包含依赖的jar包
```

## Run Jar
```
把jar复制到spark的安装目录的bin，然后运行命令 ./spark-submit --class com.client.SparkOnlineAggregationClient spark-online-aggregation-1.0-SNAPSHOT-jar-with-dependencies.jar "select max(R2) from <hdfs://localhost:9000/stream/nation.tbl> sample 0.2 confidence 0.95" 引号换成自己的sql
```

2022-3-27
改造程序，用以支持瓦片计算写入hbase，区域统计，序列分析。
改名为 Task16SparkV8TileComputing

调用：
spark-submit --master spark://xxx:7077 Task16SparkV8TileComputing.jar config17.json type task.json output.json

type: jshbase,stat,serial





2022-3-18
功能：
用于启动Spark进行xyz全部瓦片计算，结果是dataset对象，写入HBase中。

调用：
spark-submit --master spark://xxx:7077 Task16SparkV8TileComputingToHbase.jar task.json output.json

输入Json:


输出Json：
{
state:0,   //0 表示成功， 其他表示失败，错误信息查看message
message:""
}



在集群上运行JAR之前，需要提前把最新版本的libHBasePeHelperCppConnector.so拷贝到每台机器的/usr/lib下面。
在集群上运行前需要解压jar包，修改其中的task17config.json文件，适配当地集群的mysql和hbase环境。








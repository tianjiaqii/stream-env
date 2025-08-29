package com.label.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class ShopKafkaToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 1. 创建 Kafka 源表
        tableEnv.executeSql("CREATE TABLE dim_shop (\n" +
                "  `id` string,\n" +
                "  `shop_id` string,\n" +
                "  `operate_type` string,\n" +
                "  `operate_time` string,\n" +
                "  `operate_content` string,\n" +
                "  `operator_id` string\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_shop_operation_log_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 2. 创建 HBase 结果表（修复：使用正确的RowKey定义方式）
        tableEnv.executeSql("CREATE TABLE hbase_shop_dim (\n" +
                "  `rowkey` string,  -- 直接使用rowkey作为字段名，不要加列族\n" +
                "  `info` ROW<\n" +
                "    `shop_id` string,\n" +
                "    `operate_type` string,\n" +
                "    `operate_time` string,\n" +
                "    `operate_content` string,\n" +
                "    `operator_id` string\n" +
                "  >,\n" +
                "  PRIMARY KEY (`rowkey`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-2.2',\n" +
                "  'table-name' = 'hbase_shop_dim',\n" +
                "  'zookeeper.quorum' = 'cdh01:2181',\n" +
                "  'zookeeper.znode.parent' = '/hbase',\n" +
                "  'sink.buffer-flush.max-size' = '10mb',\n" +
                "  'sink.buffer-flush.max-rows' = '1000',\n" +
                "  'sink.buffer-flush.interval' = '2s'\n" +
                ")");

        // 3. 写入HBase（使用ROW构造函数）
        tableEnv.executeSql("INSERT INTO hbase_shop_dim \n" +
                "SELECT \n" +
                "  id AS `rowkey`,\n" +
                "  ROW(shop_id, operate_type, operate_time, operate_content, operator_id) AS `info`\n" +
                "FROM dim_shop");

        // 打印验证
        tableEnv.executeSql("select * from dim_shop").print();

        // 执行任务
        env.execute("Kafka to HBase Job");
    }
}

package com.label.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdActivityToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 1. 创建 Kafka 源表
        tableEnv.executeSql("CREATE TABLE dwd_marketing_activity_log (\n" +
                "  `id` string,\n" +
                "  `activity_id` string,\n" +
                "  `user_id` string,\n" +
                "  `activity_type` string,\n" +
                "  `get_time` string,\n" +
                "  `use_time` string,\n" +
                "  `coupon_amount` string,\n" +
                "  `coupon_code` string\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_marketing_activity_log_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("select * from dwd_marketing_activity_log").print();
    }
}

package com.label.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import java.time.Duration;

public class DwsUserVisitAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));

        // 创建DWD用户行为事实表（修复：WATERMARKARK → WATERMARK）
        tableEnv.executeSql("CREATE TABLE dwd_user_behavior_fact (\n" +
                "  `user_id` STRING,\n" +
                "  `visit_type` STRING,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  `is_new_visit` BOOLEAN,\n" +
                "  `is_return_visit` BOOLEAN,\n" +
                "  `is_purchase_visit` BOOLEAN,\n" +
                "  `is_follow_shop` BOOLEAN,\n" +
                "  `dt` STRING,\n" +
                "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND  -- 这里修正了拼写\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_user_behavior_fact_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dws_user_visit_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

        // 打印DWD表数据（验证读取正常）
        tableEnv.executeSql("select * from dwd_user_behavior_fact").print();

        // 创建DWS用户访问聚合表
        tableEnv.executeSql("CREATE TABLE dws_user_visit_agg (\n" +
                "  `user_id` STRING,\n" +
                "  `dt` STRING,\n" +
                "  `visit_count` BIGINT,\n" +
                "  `new_visit_count` BIGINT,\n" +
                "  `return_visit_count` BIGINT,\n" +
                "  `purchase_visit_count` BIGINT,\n" +
                "  `follow_shop_count` BIGINT,\n" +
                "  `first_visit_time` TIMESTAMP(3),\n" +
                "  `last_visit_time` TIMESTAMP(3),\n" +
                "  PRIMARY KEY (user_id, dt) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dws_user_visit_agg_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        // 按用户和天聚合访问数据
        tableEnv.executeSql("INSERT INTO dws_user_visit_agg\n" +
                "SELECT \n" +
                "  user_id,\n" +
                "  dt,\n" +
                "  COUNT(*) AS visit_count,\n" +
                "  SUM(CAST(is_new_visit AS INT)) AS new_visit_count,\n" +
                "  SUM(CAST(is_return_visit AS INT)) AS return_visit_count,\n" +
                "  SUM(CAST(is_purchase_visit AS INT)) AS purchase_visit_count,\n" +
                "  SUM(CAST(is_follow_shop AS INT)) AS follow_shop_count,\n" +
                "  MIN(event_time) AS first_visit_time,\n" +
                "  MAX(event_time) AS last_visit_time\n" +
                "FROM dwd_user_behavior_fact\n" +
                "GROUP BY user_id, dt");

        // 启动Flink作业
        env.execute("DWS User Visit Aggregation Job");
    }
}
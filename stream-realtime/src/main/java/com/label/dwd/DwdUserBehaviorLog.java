package com.label.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.time.Duration;

public class DwdUserBehaviorLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 关键：流处理作业需要设置Checkpoint（Sink端可靠写入依赖）
        env.enableCheckpointing(30000); // 每30秒做一次Checkpoint，确保数据不丢失
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));

        // 2. 创建Kafka源表（原配置正常，保留）
        tableEnv.executeSql("CREATE TABLE ods_user_behavior_log (\n" +
                "  `user_id` STRING,\n" +
                "  `device_id` STRING,\n" +
                "  `visit_type` STRING,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  `page_url` STRING,\n" +
                "  `page_type` STRING,\n" +
                "  `is_follow_shop` BOOLEAN,\n" +
                "  `proc_time` TIMESTAMP(3),\n" +
                "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_user_behavior_log_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dwd_user_behavior_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

        // 测试源表：仅用于验证数据，生产可注释
        // tableEnv.executeSql("select * from ods_user_behavior_log").print();

        // 3. 创建DWD层Kafka目标表（补充关键Sink配置）
        tableEnv.executeSql("CREATE TABLE dwd_user_behavior_fact (\n" +
                "  `user_id` STRING,\n" +
                "  `device_id` STRING,\n" +
                "  `visit_type` STRING,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  `page_type` STRING,\n" +
                "  `is_follow_shop` BOOLEAN,\n" +
                "  `is_new_visit` BOOLEAN,\n" +
                "  `is_return_visit` BOOLEAN,\n" +
                "  `is_purchase_visit` BOOLEAN,\n" +
                "  `proc_time` TIMESTAMP(3),\n" +
                "  `dt` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_user_behavior_fact_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dwd_user_behavior_sink_group',\n" + // 新增：Sink端消费者组（必填）
                "  'format' = 'json',\n" +
                "  'json.timestamp-format.standard' = 'ISO-8601',\n" + // 与源表保持格式一致，避免转换错误
                "  'sink.partitioner' = 'round-robin',\n" +
                "  'sink.delivery-guarantee' = 'at-least-once',\n" + // 新增：交付语义（确保数据写入）
                "  'sink.parallelism' = '1'\n" + // 新增：控制并行度（测试时设为1，便于排查）
                ")");

        // 4. 执行INSERT并保持作业运行（关键：流处理需显式触发作业）
        TableResult insertResult = tableEnv.executeSql("INSERT INTO dwd_user_behavior_fact\n" +
                "SELECT \n" +
                "  user_id,\n" +
                "  device_id,\n" +
                "  visit_type,\n" +
                "  event_time,\n" +
                "  page_type,\n" +
                "  is_follow_shop,\n" +
                "  visit_type = 'new' AS is_new_visit,\n" +
                "  visit_type IN ('return', 'return_no_buy') AS is_return_visit,\n" + // 优化：覆盖所有返回访问类型
                "  visit_type = 'purchase' AS is_purchase_visit,\n" +
                "  proc_time,\n" +
                "  DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt\n" +
                "FROM ods_user_behavior_log");

        // 5. 关键：流处理作业需阻塞主线程，避免作业立即退出
        insertResult.getJobClient().ifPresent(jobClient -> {
            try {
                jobClient.getJobExecutionResult().get(); // 等待作业完成（流处理会持续运行）
            } catch (Exception e) {
                throw new RuntimeException("作业执行失败", e);
            }
        });
    }
}
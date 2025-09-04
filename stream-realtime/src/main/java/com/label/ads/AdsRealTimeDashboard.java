package com.label.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdsRealTimeDashboard {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可选：降低并行度，减少资源消耗（避免之前的网络缓冲区问题）
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建DWD支付事实表（Kafka源表）
        tableEnv.executeSql("CREATE TABLE dwd_trade_payment_fact (\n" +
                "  `pay_time` TIMESTAMP(3),\n" +
                "  `pay_amount` DECIMAL(10,2),\n" +
                "  `user_id` STRING,\n" +
                "  `dt` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_trade_payment_fact_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'ads_dashboard_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 2. 创建实时大屏结果表（MySQL）
        tableEnv.executeSql("CREATE TABLE ads_realtime_dashboard (\n" +
                "  `metric_key` STRING,\n" +
                "  `metric_value` STRING,\n" +
                "  `update_time` TIMESTAMP(3),\n" +
                "  PRIMARY KEY (metric_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/ads',\n" +
                "  'table-name' = 'ads_pay',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")");

        // 3. 计算实时指标：每个INSERT INTO都要保存TableResult并await()
        // 3.1 今日GMV
        TableResult gmvResult = tableEnv.executeSql("INSERT INTO ads_realtime_dashboard\n" +
                "SELECT \n" +
                "  'today_gmv' AS metric_key,\n" +
                "  CAST(SUM(pay_amount) AS STRING) AS metric_value,\n" +
                "  NOW() AS update_time\n" +
                "FROM dwd_trade_payment_fact\n" +
                "WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')\n" +
                "GROUP BY dt");

        // 3.2 今日订单数
        TableResult orderCountResult = tableEnv.executeSql("INSERT INTO ads_realtime_dashboard\n" +
                "SELECT \n" +
                "  'today_order_count' AS metric_key,\n" +
                "  CAST(COUNT(*) AS STRING) AS metric_value,\n" +
                "  NOW() AS update_time\n" +
                "FROM dwd_trade_payment_fact\n" +
                "WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')");

        // 3.3 今日支付用户数
        TableResult customerCountResult = tableEnv.executeSql("INSERT INTO ads_realtime_dashboard\n" +
                "SELECT \n" +
                "  'today_customer_count' AS metric_key,\n" +
                "  CAST(COUNT(DISTINCT user_id) AS STRING) AS metric_value,\n" +
                "  NOW() AS update_time\n" +
                "FROM dwd_trade_payment_fact\n" +
                "WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')");

        // 4. 关键：等待所有INSERT INTO任务注册到拓扑（流式任务会持续运行）
        gmvResult.await();
        orderCountResult.await();
        customerCountResult.await();

        // 5. 启动Flink作业（此时拓扑已包含所有算子，不会为空）
        env.execute("Ads RealTime Dashboard Job");
    }
}
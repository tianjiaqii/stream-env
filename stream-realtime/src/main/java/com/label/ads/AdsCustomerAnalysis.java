package com.label.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

public class AdsCustomerAnalysis {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（不配置网络缓冲区）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 创建源表（不变）
        tableEnv.executeSql("CREATE TABLE dws_user_visit_agg (\n" +
                "  `user_id` STRING,\n" +
                "  `dt` STRING,\n" +
                "  `new_visit_count` BIGINT,\n" +
                "  `return_visit_count` BIGINT,\n" +
                "  `purchase_visit_count` BIGINT,\n" +
                "  `follow_shop_count` BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dws_user_visit_agg_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'ads_customer_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE dws_trade_payment_agg (\n" +
                "  `user_id` STRING,\n" +
                "  `dt` STRING,\n" +
                "  `payment_count` BIGINT,\n" +
                "  `payment_amount` DECIMAL(10,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dws_trade_payment_agg_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'ads_customer_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 3. 创建结果表（不变）
        tableEnv.executeSql("CREATE TABLE ads_customer_analysis (\n" +
                "  `stat_date` STRING,\n" +
                "  `new_customer_count` BIGINT,\n" +
                "  `new_customer_payment_count` BIGINT,\n" +
                "  `new_customer_payment_amount` DECIMAL(10,2),\n" +
                "  `new_customer_conversion_rate` DECIMAL(5,4),\n" +
                "  `new_customer_avg_order_value` DECIMAL(10,2),\n" +
                "  `return_customer_count` BIGINT,\n" +
                "  `return_customer_payment_count` BIGINT,\n" +
                "  `return_customer_payment_amount` DECIMAL(10,2),\n" +
                "  `return_customer_conversion_rate` DECIMAL(5,4),\n" +
                "  `return_customer_avg_order_value` DECIMAL(10,2),\n" +
                "  `old_customer_count` BIGINT,\n" +
                "  `old_customer_payment_count` BIGINT,\n" +
                "  `old_customer_payment_amount` DECIMAL(10,2),\n" +
                "  `old_customer_conversion_rate` DECIMAL(5,4),\n" +
                "  `old_customer_avg_order_value` DECIMAL(10,2),\n" +
                "  `update_time` TIMESTAMP(3),\n" +
                "  PRIMARY KEY (stat_date) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://cdh01:3306/ads',\n" +
                "  'table-name' = 'ads_customer_analysis',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")");

        // 4. 拆分计算：先创建临时视图存储各子查询结果（减少单次关联的复杂度）
        // 4.1 计算新客户指标并创建临时视图
        tableEnv.executeSql("CREATE TEMPORARY VIEW new_customer_view AS\n" +
                "SELECT \n" +
                "  v.dt,\n" +
                "  COUNT(DISTINCT v.user_id) AS new_customer_count,\n" +
                "  COUNT(DISTINCT CASE WHEN p.payment_count > 0 THEN v.user_id END) AS new_payment_customer_count,\n" +
                "  COALESCE(SUM(p.payment_amount), 0) AS new_payment_amount,\n" +
                "  COALESCE(SUM(p.payment_count), 0) AS new_payment_count\n" +
                "FROM dws_user_visit_agg v\n" +
                "LEFT JOIN dws_trade_payment_agg p ON v.user_id = p.user_id AND v.dt = p.dt\n" +
                "WHERE v.new_visit_count > 0\n" +
                "GROUP BY v.dt");

        // 4.2 计算回访客户指标并创建临时视图
        tableEnv.executeSql("CREATE TEMPORARY VIEW return_customer_view AS\n" +
                "SELECT \n" +
                "  v.dt,\n" +
                "  COUNT(DISTINCT v.user_id) AS return_customer_count,\n" +
                "  COUNT(DISTINCT CASE WHEN p.payment_count > 0 THEN v.user_id END) AS return_payment_customer_count,\n" +
                "  COALESCE(SUM(p.payment_amount), 0) AS return_payment_amount,\n" +
                "  COALESCE(SUM(p.payment_count), 0) AS return_payment_count\n" +
                "FROM dws_user_visit_agg v\n" +
                "LEFT JOIN dws_trade_payment_agg p ON v.user_id = p.user_id AND v.dt = p.dt\n" +
                "WHERE v.return_visit_count > 0 AND v.new_visit_count = 0\n" +
                "GROUP BY v.dt");

        // 4.3 计算老客户指标并创建临时视图
        tableEnv.executeSql("CREATE TEMPORARY VIEW old_customer_view AS\n" +
                "SELECT \n" +
                "  v.dt,\n" +
                "  COUNT(DISTINCT v.user_id) AS old_customer_count,\n" +
                "  COUNT(DISTINCT CASE WHEN p.payment_count > 0 THEN v.user_id END) AS old_payment_customer_count,\n" +
                "  COALESCE(SUM(p.payment_amount), 0) AS old_payment_amount,\n" +
                "  COALESCE(SUM(p.payment_count), 0) AS old_payment_count\n" +
                "FROM dws_user_visit_agg v\n" +
                "LEFT JOIN dws_trade_payment_agg p ON v.user_id = p.user_id AND v.dt = p.dt\n" +
                "WHERE v.purchase_visit_count > 0\n" +
                "GROUP BY v.dt");

        // 5. 关联临时视图，写入结果表（拆分后关联逻辑更简单，网络消耗减少）
        TableResult insertResult = tableEnv.executeSql("INSERT INTO ads_customer_analysis\n" +
                "SELECT \n" +
                "  COALESCE(n.dt, r.dt, o.dt) AS stat_date,\n" +
                "  n.new_customer_count,\n" +
                "  n.new_payment_count AS new_customer_payment_count,\n" +
                "  n.new_payment_amount AS new_customer_payment_amount,\n" +
                "  CASE WHEN n.new_customer_count > 0 THEN n.new_payment_customer_count * 1.0 / n.new_customer_count ELSE 0 END AS new_customer_conversion_rate,\n" +
                "  CASE WHEN n.new_payment_customer_count > 0 THEN n.new_payment_amount / n.new_payment_customer_count ELSE 0 END AS new_customer_avg_order_value,\n" +
                "  r.return_customer_count,\n" +
                "  r.return_payment_count AS return_customer_payment_count,\n" +
                "  r.return_payment_amount AS return_customer_payment_amount,\n" +
                "  CASE WHEN r.return_customer_count > 0 THEN r.return_payment_customer_count * 1.0 / r.return_customer_count ELSE 0 END AS return_customer_conversion_rate,\n" +
                "  CASE WHEN r.return_payment_customer_count > 0 THEN r.return_payment_amount / r.return_payment_customer_count ELSE 0 END AS return_customer_avg_order_value,\n" +
                "  o.old_customer_count,\n" +
                "  o.old_payment_count AS old_customer_payment_count,\n" +
                "  o.old_payment_amount AS old_customer_payment_amount,\n" +
                "  CASE WHEN o.old_customer_count > 0 THEN o.old_payment_customer_count * 1.0 / o.old_customer_count ELSE 0 END AS old_customer_conversion_rate,\n" +
                "  CASE WHEN o.old_payment_customer_count > 0 THEN o.old_payment_amount / o.old_payment_customer_count ELSE 0 END AS old_customer_avg_order_value,\n" +
                "  NOW() AS update_time\n" +
                "FROM new_customer_view n\n" +
                "FULL OUTER JOIN return_customer_view r ON n.dt = r.dt\n" +
                "FULL OUTER JOIN old_customer_view o ON COALESCE(n.dt, r.dt) = o.dt");

        // 6. 等待任务执行
        insertResult.await();

        // 7. （可选）查询结果
        tableEnv.executeSql("select * from ads_customer_analysis").print();
        env.execute("");
    }
}
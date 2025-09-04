package com.label.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwsTradePaymentAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建DWD支付事实表
        tableEnv.executeSql("CREATE TABLE dwd_trade_payment_fact (\n" +
                "  `pay_id` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  `pay_time` TIMESTAMP(3),\n" +
                "  `pay_amount` DECIMAL(10,2),\n" +
                "  `shop_id` STRING,\n" +
                "  `dt` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_trade_payment_fact_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dws_trade_payment_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from dwd_trade_payment_fact").print();

        // 创建DWS交易支付聚合表
        tableEnv.executeSql("CREATE TABLE dws_trade_payment_agg (\n" +
                "  `user_id` STRING,\n" +
                "  `dt` STRING,\n" +
                "  `payment_count` BIGINT,\n" +
                "  `payment_amount` DECIMAL(10,2),\n" +
                "  `shop_count` BIGINT,\n" +
                "  `first_payment_time` TIMESTAMP(3),\n" +
                "  `last_payment_time` TIMESTAMP(3),\n" +
                "  PRIMARY KEY (user_id, dt) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dws_trade_payment_agg_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");



        // 按用户和天聚合支付数据
        tableEnv.executeSql("INSERT INTO dws_trade_payment_agg\n" +
                "SELECT \n" +
                "  user_id,\n" +
                "  dt,\n" +
                "  COUNT(pay_id) AS payment_count,\n" +
                "  SUM(pay_amount) AS payment_amount,\n" +
                "  COUNT(DISTINCT shop_id) AS shop_count,\n" +
                "  MIN(pay_time) AS first_payment_time,\n" +
                "  MAX(pay_time) AS last_payment_time\n" +
                "FROM dwd_trade_payment_fact\n" +
                "GROUP BY user_id, dt");



//        env.execute("DWS Dws Trade Payment Agg Job");
    }
}

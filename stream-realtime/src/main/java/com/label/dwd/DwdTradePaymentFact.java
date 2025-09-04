package com.label.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradePaymentFact {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建支付数据源表
        tableEnv.executeSql("CREATE TABLE ods_trade_payment (\n" +
                "  `pay_id` STRING,\n" +
                "  `order_id` STRING,\n" +
                "  `pay_time` TIMESTAMP(3),\n" +
                "  `pay_amount` DECIMAL(10,2),\n" +
                "  `pay_method` STRING,\n" +
                "  `pay_status` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  WATERMARK FOR pay_time AS pay_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_trade_payment_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dwd_trade_payment_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

//        tableEnv.executeSql("select * from ods_trade_payment").print();

        // 创建订单数据源表
        tableEnv.executeSql("CREATE TABLE ods_trade_order (\n" +
                "  `order_id` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  `order_time` TIMESTAMP(3),\n" +
                "  `pay_amount` DECIMAL(10,2),\n" +
                "  `is_paid` BOOLEAN,\n" +
                "  `order_status` STRING,\n" +
                "  `goods_type` STRING,\n" +
                "  `goods_id` STRING,\n" +
                "  `shop_id` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ods_trade_order_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'dwd_trade_order_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                ")");

//        tableEnv.executeSql("select * from ods_trade_order").print();
        // 创建DWD支付事实表
        tableEnv.executeSql("CREATE TABLE dwd_trade_payment_fact (\n" +
                "  `pay_id` STRING,\n" +
                "  `order_id` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  `pay_time` TIMESTAMP(3),\n" +
                "  `pay_amount` DECIMAL(10,2),\n" +
                "  `pay_method` STRING,\n" +
                "  `pay_status` STRING,\n" +
                "  `goods_type` STRING,\n" +
                "  `goods_id` STRING,\n" +
                "  `shop_id` STRING,\n" +
                "  `dt` STRING,\n" +
                "  PRIMARY KEY (pay_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_payment_fact_topic',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        // 关联支付和订单信息
        tableEnv.executeSql("INSERT INTO dwd_trade_payment_fact\n" +
                "SELECT \n" +
                "  p.pay_id,\n" +
                "  p.order_id,\n" +
                "  p.user_id,\n" +
                "  p.pay_time,\n" +
                "  p.pay_amount,\n" +
                "  p.pay_method,\n" +
                "  p.pay_status,\n" +
                "  o.goods_type,\n" +
                "  o.goods_id,\n" +
                "  o.shop_id,\n" +
                "  DATE_FORMAT(p.pay_time, 'yyyy-MM-dd') AS dt\n" +
                "FROM ods_trade_payment p\n" +
                "LEFT JOIN ods_trade_order o ON p.order_id = o.order_id\n" +
                "WHERE p.pay_status = 'success'");
    }
}

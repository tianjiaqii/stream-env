package com.retailersv1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsOrderProvinceInfo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可根据需要设置并行度（默认使用环境配置）
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //Kafka获取dwd_order_detail_inc表数据
        tableEnv.executeSql("create table dwd_order_detail(\n" +
                "id bigint,\n" +
                "order_id bigint,\n" +
                "user_id bigint,\n" +
                "sku_id bigint,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id bigint,\n" +
                "activity_rule_id bigint,\n" +
                "coupon_id bigint,\n" +
                "date_id bigint,\n" +
                "create_time bigint,\n" +
                "sku_num bigint,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts bigint,\n" +
                "event_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
//                "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_detail_inc',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //查询表是否有数据
        Table dwd_order_detail = tableEnv.sqlQuery("select * from dwd_order_detail");
//        dwd_order_detail.execute().print();

        tableEnv.createTemporaryView("dwd_order_detail", dwd_order_detail);


        //获取HBASE表中的数据
        tableEnv.executeSql("CREATE TABLE base_province (\n" +
                " dic_code string,\n" +
                " info ROW<dic_code string , dic_name string >,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'dim_base_province',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181'\n" +
                ");");
//        tableEnv.executeSql("SELECT dic_code, info.dic_name FROM base_province").print();



        // 4. 关联两张表（通过province_id和dic_code）
        // 提取省份名称：base_province.info.dic_name
        Table joinedTable = tableEnv.sqlQuery(
                "SELECT " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "bp.info.dic_name AS province_name," +  // 获取省份名称
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "od.date_id," +
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts," +
                        "od.event_time " +
                        "FROM dwd_order_detail od " +
                        "LEFT JOIN base_province bp " +
                        "ON od.province_id = bp.dic_code");  // 关联条件：订单表的province_id = 省份表的dic_code

        // 5. 注册为临时视图，便于后续处理
        tableEnv.createTemporaryView("dws_order_province", joinedTable);

        // 6. 打印关联结果（测试用）
        joinedTable.execute().print();


        // 6. 创建Doris目标表
        tableEnv.executeSql("CREATE TABLE dws_order_province_doris (\n" +
                "id bigint,\n" +
                "order_id bigint,\n" +
                "user_id bigint,\n" +
                "sku_id bigint,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "province_name string,\n" +
                "activity_id bigint,\n" +
                "activity_rule_id bigint,\n" +
                "coupon_id bigint,\n" +
                "date_id bigint,\n" +
                "create_time bigint,\n" +
                "sku_num bigint,\n" +
                "split_activity_amount decimal(16,2),\n" +
                "split_coupon_amount decimal(16,2),\n" +
                "split_total_amount decimal(16,2),\n" +
                "ts bigint,\n" +
                "event_time timestamp(3),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +  // 以订单明细ID为主键
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = 'cdh01:7030',\n" +  // Doris前端节点地址
                "  'table.identifier' = 'gmall2023_realtime.dws_order_province_doris',\n" +  // 库名.表名
                "  'username' = 'root',\n" +  // Doris用户名
                "  'password' = '',\n" +  // 密码，默认空
                "  'sink.max-retries' = '3'\n" +  // 重试次数
                ")");


        // 7. 将关联后的数据写入Doris
        tableEnv.executeSql("INSERT INTO dws_order_province_doris " +
                "SELECT " +
                "id, order_id, user_id, sku_id, sku_name, " +
                "province_id, province_name, " +
                "activity_id, activity_rule_id, coupon_id, " +
                "date_id, create_time, sku_num, " +
                "split_activity_amount, split_coupon_amount, split_total_amount, " +
                "ts, event_time " +
                "FROM dws_order_province");

        env.execute("DwsOrderProvinceInfo");
    }
}

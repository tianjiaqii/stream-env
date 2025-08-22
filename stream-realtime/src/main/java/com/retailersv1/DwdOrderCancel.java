package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdOrderCancel {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String Kafka_topic_base_log_data = ConfigUtils.getString("kafka.cdc.db.topic");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可根据需要设置并行度（默认使用环境配置）
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        //todo 创建KafkaTable 获取Kafka中order_info的数据
        tableEnv.executeSql("CREATE TABLE KafkaOrderInfo (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `payment_way` bigint,\n"+
                "   `consignee` string,\n"+
                "   `create_time` bigint,\n"+
                "   `refundable_time` bigint,\n"+
                "   `original_total_amount` string,\n"+
                "   `coupon_reduce_amount` string,\n"+
                "   `order_status` bigint,\n"+
                "   `out_trade_no` bigint,\n"+
                "   `total_amount` string,\n"+
                "   `user_id` bigint,\n"+
                "   `province_id` bigint,\n"+
                "   `consignee_tel` string,\n"+
                "   `trade_body` string,\n"+
                "   `id` bigint,\n"+
                "   `activity_reduce_amount` string,\n"+
                "   `operate_time` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>\n" +
                //"  `ts_ms` TIMESTAMP(3),\n" +
                //"    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderDetail").print();

        //提取after中具体的值
        Table orderInfoTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`payment_way` AS payment_way,\n" +
                "`after`.`consignee` AS consignee,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`refundable_time` AS refundable_time,\n" +
                "`after`.`original_total_amount` AS original_total_amount,\n" +
                "`after`.`coupon_reduce_amount` AS coupon_reduce_amount,\n" +
                "`after`.`order_status` AS order_status,\n" +
                "`after`.`out_trade_no` AS out_trade_no,\n" +
                "`after`.`total_amount` AS total_amount,\n" +
                "`after`.`user_id` AS user_id,\n" +
                "`after`.`province_id` AS province_id,\n" +
                "`after`.`consignee_tel` AS consignee_tel,\n" +
                "`after`.`trade_body` AS trade_body,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`activity_reduce_amount` AS activity_reduce_amount,\n" +
                "`after`.`operate_time` AS operate_time\n" +
                "FROM KafkaOrderInfo where order_status=1003");

//        orderInfoTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_info_cancel", orderInfoTable);

        //Kafka获取dwd_order_detail_inc表数据
        tableEnv.executeSql("create table dwd_order_detail_inc(\n" +
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
                "ts bigint\n" +
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
        Table dwd_order_detail_inc = tableEnv.sqlQuery("select * from dwd_order_detail_inc");
//        dwd_order_detail_inc.execute().print();

        tableEnv.createTemporaryView("dwd_order_detail_inc", dwd_order_detail_inc);



        //将order_info与dwd_order_detail_inc表进行join
        Table order_cancel = tableEnv.sqlQuery("select  " +
                "od.id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "CAST(UNIX_TIMESTAMP(date_format(TO_TIMESTAMP(FROM_UNIXTIME(od.create_time / 1000)), 'yyyy-MM-dd')) AS BIGINT) AS date_id," +  // 2. 转为BIGINT，匹配目标表的date_id类型
                "oc.operate_time," +
                "od.sku_num," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "od.ts " +
                "from dwd_order_detail_inc od " +
                "join order_info_cancel oc " +
                "on od.order_id=oc.id ");

//        order_cancel.execute().print();


        //创建要写入Kafka的动态表
        tableEnv.executeSql("create table dwd_order_cancel_detail(" +
                "id bigint," +
                "order_id bigint," +
                "user_id bigint," +
                "sku_id bigint," +
                "sku_name string," +
                "province_id string," +
                "activity_id bigint," +
                "activity_rule_id bigint," +
                "coupon_id bigint," +
                "date_id bigint," +
                "cancel_time bigint," +
                "sku_num bigint," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint, " +
                "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                ") with (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'dwd_order_cancel_detail',\n" +
                "'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "'key.format' = 'json',\n" +  // 仅保留Key的格式配置
                "'value.format' = 'json',\n" +
                "'value.fields-include' = 'ALL'\n" +
                ")");

        //将关联后的数据写入创建好的动态表中
        order_cancel.executeInsert("dwd_order_cancel_detail");

        //查询数据是否写入了动态表
        tableEnv.executeSql("select * from dwd_order_cancel_detail").print();

        env.execute("DwdWideOrderCancel");
    }
}

package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdRefundPayDetail {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String Kafka_topic_base_log_data = ConfigUtils.getString("kafka.cdc.db.topic");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可根据需要设置并行度（默认使用环境配置）
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //获取HBASE表中的数据
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_code string , dic_name string >,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181'\n" +
                ");");
//        tableEnv.executeSql("SELECT dic_code, info.dic_name FROM base_dic").print();

        // 2. 使用KafkaUtils读取dim_all_topic数据创建Kafka数据源
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        Kafka_topic_base_log_data,
                        "group_id",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_dim_all_topic"
        );
//        kafkaSourceDs.print("kafkaSourceDs -> ");

        //过滤refund_payment评论表的数据
        SingleOutputStreamOperator<JSONObject> refundPaymentJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("refund_payment"))
                .uid("filter_refund_payment data")
                .name("filter_refund_payment data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> refundPaymentDs = refundPaymentJsonDs.map(JSON::toJSONString);

//        refundPaymentDs.print("refundPaymentDs -> ");
        //发送到dwd_order_refund_info主题
        refundPaymentDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_refund_payment"));

        //创建KafkaTable 获取Kafka中的数据
        tableEnv.executeSql("CREATE TABLE KafkaRefundPaymentTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `callback_time` bigint,\n"+
                "   `payment_type` string,\n"+
                "   `refund_status` string,\n"+
                "   `create_time` bigint,\n"+
                "   `total_amount` string,\n"+
                "   `subject` string,\n"+
                "   `trade_no` string,\n"+
                "   `sku_id` bigint,\n"+
                "   `callback_content` string,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint,\n"+
                "   `operate_time` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`create_time` / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_refund_payment',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderRefundInfoTable").print();

        //提取after中具体的值
        Table RefundPayTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`callback_time` AS callback_time,\n" +
                "`after`.`payment_type` AS payment_type,\n" +
                "`after`.`refund_status` AS refund_status,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`total_amount` AS total_amount,\n" +
                "`after`.`subject` AS subject,\n" +
                "`after`.`trade_no` AS trade_no,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`callback_content` AS callback_content,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`after`.`operate_time` AS operate_time,\n" +
                "`event_time` AS ts ,\n" +
                "`proc_time` AS td \n" +
                "FROM KafkaRefundPaymentTable where refund_status='1602'");

//        RefundPayTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_refund_pay", RefundPayTable);



        tableEnv.executeSql("CREATE TABLE KafkaOrderRefundInfoTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `refund_type` string,\n"+
                "   `refund_status` string,\n"+
                "   `create_time` bigint,\n"+
                "   `user_id` bigint,\n"+
                "   `refund_num` bigint,\n"+
                "   `refund_reason_type` string,\n"+
                "   `refund_amount` string,\n"+
                "   `sku_id` bigint,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint,\n"+
                "   `refund_reason_txt` string"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`create_time` / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_refund_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderRefundInfoTable").print();

        //提取after中具体的值
        Table orderRefundTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`refund_type` AS refund_type,\n" +
                "`after`.`refund_status` AS refund_status,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`user_id` AS user_id,\n" +
                "`after`.`refund_num` AS refund_num,\n" +
                "`after`.`refund_reason_type` AS refund_reason_type,\n" +
                "`after`.`refund_amount` AS refund_amount,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`after`.`refund_reason_txt` AS refund_reason_txt,\n" +
                "`event_time` AS ts ,\n" +
                "`proc_time` AS td \n" +
                "FROM KafkaOrderRefundInfoTable where refund_status = '0705'");

//        orderRefundTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_refund_info", orderRefundTable);



        //todo 过滤orderInfo的数据
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
                "FROM KafkaOrderInfo where order_status=1006");

//        orderInfoTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_info", orderInfoTable);



        Table orderRefundPay = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "CAST(UNIX_TIMESTAMP(date_format(TO_TIMESTAMP(FROM_UNIXTIME(rp.callback_time / 1000)), 'yyyy-MM-dd')) AS BIGINT) AS date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from order_refund_pay rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.td as dic " +
                        "on rp.payment_type=dic.dic_code ");

//        orderRefundPay.execute().print();


        tableEnv.executeSql(
                "create table dwd_refund_pay_inc(" +
                        "id bigint," +
                        "user_id bigint," +
                        "order_id bigint," +
                        "sku_id bigint," +
                        "province_id bigint," +
                        "payment_type string," +
                        "payment_type_name string," +
                        "date_id bigint," +
                        "callback_time bigint," +
                        "refund_num bigint," +
                        "total_amount string," +
                        "ts TIMESTAMP(3)," +
                        "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                        ")with (\n" +
                        "'connector' = 'upsert-kafka',\n" +
                        "'topic' = 'dwd_refund_pay_inc',\n" +
                        "'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                        "'key.format' = 'json',\n" +  // 仅保留Key的格式配置
                        "'value.format' = 'json',\n" +
                        "'value.fields-include' = 'ALL'\n" +
                        ")");

        //将关联后的数据写入创建好的动态表中
        orderRefundPay.executeInsert("dwd_refund_pay_inc");

        //查询数据是否写入了动态表
        tableEnv.executeSql("select * from dwd_refund_pay_inc").print();

        env.execute("DwdRefundPayDetail");
    }
}

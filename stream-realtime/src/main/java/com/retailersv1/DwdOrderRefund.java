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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdOrderRefund {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String Kafka_topic_base_log_data = ConfigUtils.getString("kafka.cdc.db.topic");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 可根据需要设置并行度（默认使用环境配置）
        env.setParallelism(2);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        //过滤orderRefundInfo评论表的数据
        SingleOutputStreamOperator<JSONObject> orderRefundInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("order_refund_info"))
                .uid("filter_payment_info data")
                .name("filter_payment_info data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> orderRefundInfoDs = orderRefundInfoJsonDs.map(JSON::toJSONString);

//        orderRefundInfoDs.print("orderRefundInfoDs -> ");
        //发送到dwd_order_refund_info主题
        orderRefundInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_order_refund_info"));

        //创建KafkaTable 获取Kafka中的数据
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
                "FROM KafkaOrderRefundInfoTable");

//        orderRefundTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_refund_info", orderRefundTable);


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
                "FROM KafkaOrderInfo where order_status=1005");

//        orderInfoTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_info", orderInfoTable);


        Table orderRefund = tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "CAST(UNIX_TIMESTAMP(date_format(TO_TIMESTAMP(FROM_UNIXTIME(ri.create_time / 1000)), 'yyyy-MM-dd')) AS BIGINT) AS date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.td as dic1 " +
                        "on CAST(oi.order_status AS STRING)=dic1.dic_code ");


//        orderRefund.execute().print();


         tableEnv.executeSql(
                "create table dwd_order_refund_inc(" +
                        "id bigint," +
                        "user_id bigint," +
                        "order_id bigint," +
                        "sku_id bigint," +
                        "province_id bigint," +
                        "date_id bigint," +
                        "create_time bigint," +
                        "refund_type string," +
                        "dic_name string," +
                        "refund_reason_type string," +
                        "refund_reason_txt string," +
                        "refund_num bigint," +
                        "refund_amount string," +
                        "ts TIMESTAMP(3)," +
                        "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                        ")with (\n" +
                        "'connector' = 'upsert-kafka',\n" +
                        "'topic' = 'dwd_order_refund_inc',\n" +
                        "'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                        "'key.format' = 'json',\n" +  // 仅保留Key的格式配置
                        "'value.format' = 'json',\n" +
                        "'value.fields-include' = 'ALL'\n" +
                        ")");

        //将关联后的数据写入创建好的动态表中
        orderRefund.executeInsert("dwd_order_refund_inc");

        //查询数据是否写入了动态表
        tableEnv.executeSql("select * from dwd_order_refund_inc").print();

        env.execute("DwdOrderRefund");
    }
}

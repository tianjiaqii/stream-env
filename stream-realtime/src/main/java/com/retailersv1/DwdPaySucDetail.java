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

public class DwdPaySucDetail {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String Kafka_topic_base_log_data = ConfigUtils.getString("kafka.cdc.db.topic");
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
        Table dwd_order_detail = tableEnv.sqlQuery("select * from dwd_order_detail");
//        dwd_order_detail.execute().print();

        tableEnv.createTemporaryView("dwd_order_detail", dwd_order_detail);


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

        //过滤paymentInfo评论表的数据
        SingleOutputStreamOperator<JSONObject> paymentInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("payment_info"))
                .uid("filter_payment_info data")
                .name("filter_payment_info data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> paymentInfoDs = paymentInfoJsonDs.map(JSON::toJSONString);

//        paymentInfoDs.print("paymentInfoDs -> ");
        //发送到dwd_payment_info主题
        paymentInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_payment_info"));

        //创建KafkaTable 获取Kafka中的数据
        tableEnv.executeSql("CREATE TABLE KafkaPaymentTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `callback_time` bigint,\n"+
                "   `payment_type` string,\n"+
                "   `out_trade_no` string,\n"+
                "   `create_time` bigint,\n"+
                "   `user_id` bigint,\n"+
                "   `total_amount` string,\n"+
                "   `subject` string,\n"+
                "   `payment_status` string,\n"+
                "   `callback_content` string,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint,\n"+
                "   `operate_time` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "    ts_ts AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms / 1000)),\n" +
                "    WATERMARK FOR ts_ts AS ts_ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_payment_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaPaymentTable").print();

        //提取after中具体的值
        Table paymentTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`callback_time` AS callback_time,\n" +
                "`after`.`payment_type` AS payment_type,\n" +
                "`after`.`out_trade_no` AS out_trade_no,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`user_id` AS user_id,\n" +
                "`after`.`total_amount` AS total_amount,\n" +
                "`after`.`subject` AS subject,\n" +
                "`after`.`payment_status` AS payment_status,\n" +
                "`after`.`callback_content` AS callback_content,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`after`.`operate_time` AS operate_time,\n" +
                "`proc_time` AS ts \n" +
                "FROM KafkaPaymentTable where payment_status='1602'");

//        paymentTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("payment_info", paymentTable);



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
//        tableEnv.executeSql("SELECT info.dic_code, info.dic_name FROM base_dic").print();


        Table paymentSuccessInfo = tableEnv.sqlQuery("select " +
                "od.id order_detail_id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "pi.payment_type payment_type_code ," +
                "dic.dic_name payment_type_name," +
                "pi.callback_time," +
                "od.sku_num," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount split_payment_amount," +
                "pi.ts " +
                "from payment_info pi " +
                "join dwd_order_detail od " +
                "on pi.order_id=od.order_id " +
//                "and od.et >= pi.et - interval '30' minute " +
//                "and od.et <= pi.et + interval '5' second " +
                "join base_dic for system_time as of pi.ts as dic " +
                "on pi.payment_type=dic.dic_code ");

        paymentSuccessInfo.execute().print();



        env.execute("DwdPaySucDetail");





    }
}

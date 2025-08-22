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

public class DwdOrderDetail {
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


        //todo 过滤orderDetail的数据
        SingleOutputStreamOperator<JSONObject> orderDetailJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter_order_detail data")
                .name("filter_order_detail data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> orderDetailDs = orderDetailJsonDs.map(JSON::toJSONString);

//        orderDetailDs.print("orderDetailDs -> ");
        //发送到dwd_order_detail主题
        orderDetailDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_order_detail"));


        //todo 过滤orderInfo的数据
        SingleOutputStreamOperator<JSONObject> orderInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter_order_info data")
                .name("filter_order_info data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> orderInfoDs = orderInfoJsonDs.map(JSON::toJSONString);

//        orderInfoDs.print("orderInfoDs -> ");
        //发送到dwd_order_info主题
        orderInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_order_info"));


        //todo 过滤order_detail_activity的数据
        SingleOutputStreamOperator<JSONObject> orderDetailActivityJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("order_detail_activity"))
                .uid("filter_order_detail_activity data")
                .name("filter_order_detail_activity data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> orderDetailActivityDs = orderDetailActivityJsonDs.map(JSON::toJSONString);

//        orderDetailActivityDs.print("orderDetailActivityDs -> ");
        //发送到dwd_order_info主题
        orderDetailActivityDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_order_detail_activity"));


        //todo 过滤order_detail_coupon的数据
        SingleOutputStreamOperator<JSONObject> orderDetailCouponJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("order_detail_coupon"))
                .uid("filter_order_detail_coupon data")
                .name("filter_order_detail_coupon data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> orderDetailCouponDs = orderDetailCouponJsonDs.map(JSON::toJSONString);

//        orderDetailCouponDs.print("orderDetailCouponDs -> ");
        //发送到dwd_order_info主题
        orderDetailCouponDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_order_detail_coupon"));



        //todo 创建KafkaTable 获取Kafka中order_detail的数据
        tableEnv.executeSql("CREATE TABLE KafkaOrderDetail (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `sku_num` bigint,\n"+
                "   `create_time` bigint,\n"+
                "   `split_coupon_amount` string,\n"+
                "   `sku_id` bigint,\n"+
                "   `sku_name` string,\n"+
                "   `order_price` string,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint,\n"+
                "   `split_activity_amount` string,\n"+
                "   `split_total_amount` string"+
                ">,\n"+
                "  `source` MAP<string,string>\n" +
                //"  `ts_ms` TIMESTAMP(3),\n" +
                //"    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_detail',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderDetail").print();

        //提取after中具体的值
        Table orderDetailTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`sku_num` AS sku_num,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`split_coupon_amount` AS split_coupon_amount,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`sku_name` AS sku_name,\n" +
                "`after`.`order_price` AS order_price,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`after`.`split_activity_amount` AS split_activity_amount,\n" +
                "`after`.`split_total_amount` AS split_total_amount\n" +
                "FROM KafkaOrderDetail");

//        orderDetailTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_detail", orderDetailTable);



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
                "FROM KafkaOrderInfo");

//        orderInfoTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_info", orderInfoTable);



        //todo 创建KafkaTable 获取Kafka中order_detail_activity的数据
        tableEnv.executeSql("CREATE TABLE KafkaOrderDetailActivity (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `order_detail_id` bigint,\n"+
                "   `create_time` bigint,\n"+
                "   `activity_rule_id` bigint,\n"+
                "   `activity_id` bigint,\n"+
                "   `sku_id` bigint,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>\n" +
                //"  `ts_ms` TIMESTAMP(3),\n" +
                //"    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_detail_activity',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderDetail").print();

        //提取after中具体的值
        Table orderDetailActivityTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`order_detail_id` AS order_detail_id,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`activity_rule_id` AS activity_rule_id,\n" +
                "`after`.`activity_id` AS activity_id,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id\n" +
                "FROM KafkaOrderDetailActivity");

//        orderDetailTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivityTable);



        //todo 创建KafkaTable 获取Kafka中order_detail_Coupon的数据
        tableEnv.executeSql("CREATE TABLE KafkaOrderDetailCoupon(\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `order_detail_id` bigint,\n"+
                "   `coupon_id` bigint,\n"+
                "   `create_time` bigint,\n"+
                "   `sku_id` bigint,\n"+
                "   `id` bigint,\n"+
                "   `order_id` bigint,\n"+
                "   `coupon_use_id` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>\n" +
                //"  `ts_ms` TIMESTAMP(3),\n" +
                //"    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_order_detail_coupon',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaOrderDetail").print();

        //提取after中具体的值
        Table orderDetailCouponTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`order_detail_id` AS order_detail_id,\n" +
                "`after`.`coupon_id` AS coupon_id,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`after`.`coupon_use_id` AS coupon_use_id\n" +
                "FROM KafkaOrderDetailCoupon");

//        orderDetailCouponTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCouponTable);


        //关联四张表，主表为order_detail
        Table WideOrderDetail = tableEnv.sqlQuery("select " +
                "od.id," +
                "od.order_id," +
                "oi.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "CAST(oi.province_id AS STRING) AS province_id," +  // 1. 转为STRING，匹配目标表的province_id类型
                "act.activity_id," +
                "act.activity_rule_id," +
                "cou.coupon_id," +
                "CAST(UNIX_TIMESTAMP(date_format(TO_TIMESTAMP(FROM_UNIXTIME(od.create_time / 1000)), 'yyyy-MM-dd')) AS BIGINT) AS date_id," +  // 2. 转为BIGINT，匹配目标表的date_id类型
                "od.create_time," +
                "od.sku_num," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +  // 4. 保留一个split_total_amount，删除重复
                "od.create_time AS ts " +  // 5. 别名改为ts，匹配目标表的ts字段
                "from order_detail od " +
                "join order_info oi on od.order_id = oi.id " +  // 补充空格，修复SQL语法
                "left join order_detail_activity act " +
                "on od.id = act.order_detail_id " +  // 补充空格，修复SQL语法
                "left join order_detail_coupon cou " +
                "on od.id = cou.order_detail_id ");  // 补充空格，修复SQL语法

//        WideOrderDetail.execute().print();


        //创建要写入Kafka的动态表
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
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                ") with (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'dwd_order_detail_inc',\n" +
                "'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "'key.format' = 'json',\n" +  // 仅保留Key的格式配置
                "'value.format' = 'json',\n" +
                "'value.fields-include' = 'ALL'\n" +
                ")");

        //将关联后的数据写入创建好的动态表中
        WideOrderDetail.executeInsert("dwd_order_detail_inc");

        //查询数据是否写入了动态表
        tableEnv.executeSql("select * from dwd_order_detail_inc").print();



        env.execute("DwdWideOrderDetail");
    }
}

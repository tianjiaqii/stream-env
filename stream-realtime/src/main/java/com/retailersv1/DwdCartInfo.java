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

public class DwdCartInfo {
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

        //过滤cartInfo评论表的数据
        SingleOutputStreamOperator<JSONObject> cartInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("cart_info"))
                .uid("filter_cart_info data")
                .name("filter_cart_info data");
        
        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> cartInfoDs = cartInfoJsonDs.map(JSON::toJSONString);

//        cartInfoDs.print("cartInfoDs -> ");

        //发送到dwd_cart_info主题
        cartInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_cart_info"));

        //创建KafkaTable 获取Kafka中的数据
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `is_ordered` string,\n"+
                "   `cart_price` string,\n"+
                "   `sku_num` bigint,\n"+
                "   `create_time` bigint,\n"+
                "   `user_id` string,\n"+
                "   `sku_id` string,\n"+
                "   `sku_name` string,\n"+
                "   `id` string,\n"+
                "   `operate_time` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "    ts_ts AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms / 1000)),\n" +
                "    WATERMARK FOR ts_ts AS ts_ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_cart_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaTable").print();

        //提取after中具体的值
        Table cartTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`is_ordered` AS is_ordered,\n" +
                "`after`.`cart_price` AS cart_price,\n" +
                "`after`.`sku_num` AS sku_num,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`user_id` AS user_id,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`sku_name` AS sku_name,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`operate_time` AS operate_time,\n" +
                "`proc_time` AS ts \n" +
                "FROM KafkaTable");

//        cartTable.execute().print();

        //创建要写入Kafka的动态表
        tableEnv.executeSql("create table dwd_cart_info_inc(\n" +
                "is_ordered string,\n" +
                "cart_price string,\n" +
                "sku_num bigint,\n" +
                "create_time bigint,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "id string,\n"+
                "operate_time bigint,\n"+
                "ts TIMESTAMP_LTZ(3),\n"+
                "PRIMARY KEY (id) NOT ENFORCED  -- 主键会自动作为Kafka的Key\n" +
                ") with (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'dwd_cart_info_inc',\n" +
                "'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "'key.format' = 'json',\n" +  // 仅保留Key的格式配置
                "'value.format' = 'json',\n" +
                "'value.fields-include' = 'ALL'\n" +
                ")");

        //将关联后的数据写入创建好的动态表中
        cartTable.executeInsert("dwd_cart_info_inc");

        //查询数据是否写入了动态表
        tableEnv.executeSql("select * from dwd_cart_info_inc").print();

        env.execute("dwd_cart_info_inc");
    }
}

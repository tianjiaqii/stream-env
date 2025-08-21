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


import java.util.Date;

public class DwdCommentInfo {
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


        //过滤commentInfo评论表的数据
        SingleOutputStreamOperator<JSONObject> commentInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("comment_info"))
                .uid("filter_comment_info data")
                .name("filter_comment_info data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> commentInfoDs = commentInfoJsonDs.map(JSON::toJSONString);

//        commentInfoDs.print("commentInfoDs -> ");
        //发送到dwd_comment_info主题
        commentInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dwd_comment_info"));

        //创建KafkaTable 获取Kafka中的数据
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `create_time` bigint,\n"+
                "   `user_id` string,\n"+
                "   `appraise` string,\n"+
                "   `comment_txt` string,\n"+
                "   `nick_name` string,\n"+
                "   `sku_id` string,\n"+
                "   `id` string,\n"+
                "   `spu_id` string,\n"+
                "   `order_id` string"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "    ts_ts AS TO_TIMESTAMP(FROM_UNIXTIME(ts_ms / 1000)),\n" +
                "    WATERMARK FOR ts_ts AS ts_ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_comment_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaTable").print();

        //提取after中具体的值
        Table commentTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`user_id` AS user_id,\n" +
                "`after`.`appraise` AS appraise,\n" +
                "`after`.`comment_txt` AS comment_txt,\n" +
                "`after`.`nick_name` AS nick_name,\n" +
                "`after`.`sku_id` AS sku_id,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`spu_id` AS spu_id,\n" +
                "`after`.`order_id` AS order_id,\n" +
                "`proc_time` AS ts \n" +
                "FROM KafkaTable");

//        commentTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info", commentTable);

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


        //将评论表和字典表进行关联
        Table joinTable = tableEnv.sqlQuery("SELECT \n" +
                "id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "appraise,\n" +
                "dic.dic_name appraise_name,\n" +
                "comment_txt,\n" +
                "c.ts ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.ts AS dic\n" +
                "    ON c.appraise = dic.dic_code;");

        joinTable.execute().print();


        //创建要写入Kafka的动态表
        tableEnv.executeSql("create table dwd_comment_info_inc(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "appraise string,\n" +
                "appraise_name string,\n" +
                "comment_txt string,\n" +
                "ts TIMESTAMP_LTZ(3),\n" +
                "PRIMARY KEY (id) NOT ENFORCED  -- 关键：添加主键约束（以id为例）\n" +
                ") with (\n" +
                "'connector' = 'upsert-kafka',\n" +
                "'topic' = 'dwd_comment_info_inc_topic',\n" +
                "'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "'key.format' = 'json',\n" +
                "'value.format' = 'json'\n" +
                ")");

        joinTable.executeInsert("dwd_comment_info_inc");

        // 4. 执行Flink作业
        env.execute("Read Kafka dim_all_topic Data");



    }
}

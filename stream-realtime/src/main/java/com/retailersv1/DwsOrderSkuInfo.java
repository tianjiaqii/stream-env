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

public class DwsOrderSkuInfo {
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


        //使用KafkaUtils读取dim_all_topic数据创建Kafka数据源
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

        //过滤sku_info评论表的数据
        SingleOutputStreamOperator<JSONObject> skuInfoJsonDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("sku_info"))
                .uid("filter_sku_info data")
                .name("filter_sku_info data");

        //将jsonobject数据转为string
        SingleOutputStreamOperator<String> skuInfoDs = skuInfoJsonDs.map(JSON::toJSONString);

//        skuInfoDs.print("skuInfoDs -> ");
        //发送到dws_sku_info主题
        skuInfoDs.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092","dws_sku_info"));

        //创建KafkaTable 获取Kafka中的数据
        tableEnv.executeSql("CREATE TABLE KafkaSkuInfoTable (\n" +
                "  `op` string,\n" +
                "  `after` ROW<\n" +
                "   `is_sale` bigint,\n"+
                "   `sku_desc` string,\n"+
                "   `tm_id` bigint,\n"+
                "   `create_time` bigint,\n"+
                "   `price` string,\n"+
                "   `sku_default_img` string,\n"+
                "   `weight` string,\n"+
                "   `sku_name` string,\n"+
                "   `id` bigint,\n"+
                "   `spu_id` bigint,\n"+
                "   `category3_id` bigint"+
                ">,\n"+
                "  `source` MAP<string,string>,\n" +
                "   `proc_time` as proctime() ,\n"+
                "  `ts_ms` BIGINT,\n" +
                "   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`create_time` / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dws_sku_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

//        tableEnv.executeSql("select * from KafkaSkuInfoTable").print();

        //提取after中具体的值
        Table skuTable = tableEnv.sqlQuery("SELECT \n" +
                "`after`.`is_sale` AS is_sale,\n" +
                "`after`.`sku_desc` AS sku_desc,\n" +
                "`after`.`tm_id` AS tm_id,\n" +
                "`after`.`create_time` AS create_time,\n" +
                "`after`.`price` AS price,\n" +
                "`after`.`sku_default_img` AS sku_default_img,\n" +
                "`after`.`weight` AS weight,\n" +
                "`after`.`sku_name` AS sku_name,\n" +
                "`after`.`id` AS id,\n" +
                "`after`.`spu_id` AS spu_id,\n" +
                "`after`.`category3_id` AS category3_id,\n" +
                "`event_time` AS ts ,\n" +
                "`proc_time` AS td \n" +
                "FROM KafkaSkuInfoTable");

//        skuTable.execute().print();

        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("sku_info", skuTable);


        //获取HBASE表中的数据
        tableEnv.executeSql("CREATE TABLE base_trademark (\n" +
                " dic_code string,\n" +
                " info ROW<dic_code string , dic_name string >,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'dim_base_trademark',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181'\n" +
                ");");
//        tableEnv.executeSql("SELECT dic_code, info.dic_name FROM base_trademark").print();



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
//        joinedTable.execute().print();


        // 关联逻辑：订单-省份表的sku_id → 商品表的sku_id，补充商品表的tm_id等字段
        Table orderProvinceSkuJoinTable = tableEnv.sqlQuery(
                "SELECT " +
                        // 1. 保留订单-省份表的所有核心字段（字段列表末尾无多余逗号）
                        "op.id," +
                        "op.order_id," +
                        "op.user_id," +
                        "op.sku_id," +          // 关联字段（与商品表sku_id一致）
                        "sku.sku_name," +     // 订单表商品名（区分商品表）
                        "op.province_id," +
                        "op.province_name," +
                        "op.activity_id," +
                        "op.activity_rule_id," +
                        "op.coupon_id," +
                        "op.date_id," +
                        "op.create_time," +
                        "op.sku_num," +
                        "op.split_activity_amount," +
                        "op.split_coupon_amount," +
                        "op.split_total_amount," +
                        "op.ts," +
                        "op.event_time," +
                        // 2. 补充商品表字段（核心：tm_id；字段后加空格，与FROM分隔）
                        "sku.tm_id " +          // 关键修正：末尾加空格，避免与FROM拼接
                        // 3. 关联条件（修正：商品表主键是sku_id，不是id）
                        "FROM dws_order_province op " +  // 订单-省份表别名op
                        "LEFT JOIN sku_info sku " +     // 左连接（保留所有订单，即使商品信息缺失）
                        "ON op.sku_id = sku.id");   // 关键修正：关联字段改为sku.sku_id



// 1. 打印关联结果（测试用，查看是否成功补充tm_id）
//        orderProvinceSkuJoinTable.limit(10).execute().print();  // 打印前10条，避免数据过多

// 2. 注册为临时视图，便于后续使用（如关联品牌表、写入存储等）
        tableEnv.createTemporaryView("dws_order_province_sku", orderProvinceSkuJoinTable);


        Table finalJoinTable = tableEnv.sqlQuery(
                "SELECT " +
                        // 保留订单-省份-商品表的所有字段
                        "ops.id," +
                        "ops.order_id," +
                        "ops.user_id," +
                        "ops.sku_id," +
                        "ops.sku_name," +
                        "ops.province_id," +
                        "ops.province_name," +
                        "ops.activity_id," +
                        "ops.activity_rule_id," +
                        "ops.coupon_id," +
                        "ops.date_id," +
                        "ops.create_time," +
                        "ops.sku_num," +
                        "ops.split_activity_amount," +
                        "ops.split_coupon_amount," +
                        "ops.split_total_amount," +
                        "ops.ts," +
                        "ops.event_time," +
                        "ops.tm_id," +  // 商品品牌ID
                        // 补充品牌表字段（核心：品牌名称info.dic_name）
                        "COALESCE(tm.info.dic_name, '未知品牌') AS tm_name " +
                // 关联条件：商品表tm_id（转string） = 品牌表dic_code
                "FROM dws_order_province_sku ops " +  // 订单-省份-商品表别名ops
                "LEFT JOIN base_trademark tm " +  // 左连接（保留所有订单，即使品牌信息缺失）
                "ON CAST(ops.tm_id AS STRING) = tm.dic_code");  // 关键：类型转换后关联

//将表对象注册到表执行环境中
        tableEnv.createTemporaryView("final_join_view", finalJoinTable);

        // 打印前10条结果，确认品牌名称tm_name是否正确关联
        System.out.println("四表关联最终结果（含品牌名称）：");
//        finalJoinTable.execute().print();




        // 2.1 配置MySQL连接参数（替换为您的MySQL实际配置）
        String mysqlUrl = "jdbc:mysql://cdh01:3306/gmall08?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";
        String mysqlUser = "root";
        String mysqlPwd = "123456";  // 替换为您的MySQL密码
        String mysqlTable = "dws_order_province_sku_trademark";  // MySQL目标表名

        // 2.2 在Flink中注册MySQL目标表（字段与finalJoinTable完全匹配）
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "id BIGINT COMMENT '订单明细ID',\n" +
                "order_id BIGINT COMMENT '订单ID',\n" +
                "user_id BIGINT COMMENT '用户ID',\n" +
                "sku_id BIGINT COMMENT '商品SKU ID',\n" +
                "sku_name VARCHAR(200) COMMENT '商品名称',\n" +
                "province_id VARCHAR(20) COMMENT '省份ID',\n" +
                "province_name VARCHAR(50) COMMENT '省份名称',\n" +
                "activity_id BIGINT COMMENT '活动ID',\n" +
                "activity_rule_id BIGINT COMMENT '活动规则ID',\n" +
                "coupon_id BIGINT COMMENT '优惠券ID',\n" +
                "date_id BIGINT COMMENT '日期ID（yyyyMMdd）',\n" +
                "create_time BIGINT COMMENT '订单创建时间（毫秒时间戳）',\n" +
                "sku_num BIGINT COMMENT '商品购买数量',\n" +
                "split_activity_amount VARCHAR(50) COMMENT '活动分摊金额',\n" +
                "split_coupon_amount VARCHAR(50) COMMENT '优惠券分摊金额',\n" +
                "split_total_amount VARCHAR(50) COMMENT '订单明细总金额',\n" +
                "ts BIGINT COMMENT '数据时间戳',\n" +
                "event_time TIMESTAMP COMMENT '订单事件时间',\n" +
                "tm_id BIGINT COMMENT '品牌ID',\n" +
                "tm_name VARCHAR(50) COMMENT '品牌名称',\n" +
                "PRIMARY KEY (id) NOT ENFORCED -- MySQL主键（确保订单明细ID唯一）\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +  // Flink JDBC连接器（支持MySQL）
                "  'url' = '" + mysqlUrl + "',\n" +
                "  'table-name' = '" + mysqlTable + "',\n" +
                "  'username' = '" + mysqlUser + "',\n" +
                "  'password' = '" + mysqlPwd + "',\n" +
                // 批量写入优化（关键：提升写入性能）
                "  'sink.max-retries' = '3'\n" +  // 写入失败重试3次
                ")");

        System.out.println("开始将四表关联结果写入MySQL...");
        tableEnv.executeSql("INSERT INTO " + mysqlTable + " " +
                "SELECT " +
                "id, order_id, user_id, sku_id, sku_name, " +
                "province_id, province_name, activity_id, activity_rule_id, coupon_id, " +
                "date_id, create_time, sku_num, " +
                "split_activity_amount, split_coupon_amount, split_total_amount, " +
                "ts, event_time, tm_id, tm_name " +
                "FROM final_join_view");

        // 2.4 验证写入结果（可选）
        Table mysqlResult = tableEnv.sqlQuery("SELECT * FROM " + mysqlTable + " ORDER BY create_time DESC LIMIT 10");
        System.out.println("MySQL写入结果（最新10条）：");
        mysqlResult.execute().print();




        env.execute("DwsSkuOrderInfo");
    }
}

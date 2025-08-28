package com.label.ods;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.json.JSONObject;

import java.util.Properties;

public class OdsCDCToKafka {
    // MySQL连接信息（请确认与实际环境一致）
    private static final String MYSQL_URL = "jdbc:mysql://cdh01:3306/test?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    // Kafka连接信息（请确认与实际环境一致）
    private static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static void main(String[] args) throws Exception {
        // 初始化Flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用Checkpoint（容错，可选但建议开启）
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 处理用户行为日志表
        processTable(tEnv, env,
                "ods_user_behavior_log",
                "ods_user_behavior_log_topic",
                new String[]{"user_id", "device_id", "visit_type", "event_time", "page_url", "page_type", "is_follow_shop", "proc_time"});

        // 2. 处理交易订单表
        processTable(tEnv, env,
                "ods_trade_order",
                "ods_trade_order_topic",
                new String[]{"order_id", "user_id", "order_time", "pay_amount", "is_paid", "order_status", "goods_type", "goods_id", "shop_id"});

        // 3. 处理交易支付表
        processTable(tEnv, env,
                "ods_trade_payment",
                "ods_trade_payment_topic",
                new String[]{"pay_id", "order_id", "pay_time", "pay_amount", "pay_method", "pay_status", "user_id"});

        // 4. 处理商品运营日志表
        processTable(tEnv, env,
                "ods_goods_operation_log",
                "ods_goods_operation_log_topic",
                new String[]{"id", "goods_id", "operate_type", "operate_time", "price", "stock_num", "operate_content"});

        // 5. 处理营销活动日志表
        processTable(tEnv, env,
                "ods_marketing_activity_log",
                "ods_marketing_activity_log_topic",
                new String[]{"id", "activity_id", "user_id", "activity_type", "get_time", "use_time", "coupon_amount", "coupon_code"});

        // 6. 处理用户注册日志表
        processTable(tEnv, env,
                "ods_user_register_log",
                "ods_user_register_log_topic",
                new String[]{"user_id", "register_time", "register_channel", "device_id", "register_ip"});

        // 7. 处理店铺运营日志表
        processTable(tEnv, env,
                "ods_shop_operation_log",
                "ods_shop_operation_log_topic",
                new String[]{"id", "shop_id", "operate_type", "operate_time", "operate_content", "operator_id"});

        env.execute("Flink MySQL to Kafka");
    }

    /**
     * 通用处理方法：读取表数据→转换为JSON→发送到Kafka
     */
    private static void processTable(StreamTableEnvironment tEnv, StreamExecutionEnvironment env,
                                     String tableName, String kafkaTopic, String[] columnNames) {
        // 创建Flink表与MySQL表的映射（核心：字段类型精准匹配）
        String createTableSql = createTableDDL(tableName, columnNames);
        tEnv.executeSql(createTableSql);

        // 读取表数据
        Table table = tEnv.sqlQuery("SELECT * FROM " + tableName);

        // 转换为DataStream并转为JSON（处理null值避免异常）
        DataStream<String> jsonStream = tEnv.toDataStream(table)
                .map(new RowToJsonMapFunction(columnNames))
                .name("Convert " + tableName + " to JSON");

        // 发送到Kafka（至少一次语义）
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                kafkaTopic,
                new SimpleStringSchema(),
                getKafkaProperties()
        );
        producer.setWriteTimestampToKafka(true);
        jsonStream.addSink(producer).name("Sink to " + kafkaTopic);
    }

    /**
     * 生成建表DDL（关键：根据MySQL表结构精准定义Flink字段类型）
     */
    private static String createTableDDL(String tableName, String[] columnNames) {
        StringBuilder columns = new StringBuilder();
        for (String column : columnNames) {
            String type = getColumnType(column); // 精准获取字段类型
            columns.append(column).append(" ").append(type).append(",");
        }
        // 移除最后一个逗号
        if (columns.length() > 0) {
            columns.setLength(columns.length() - 1);
        }

        return "CREATE TABLE " + tableName + " (" +
                columns.toString() +
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = '" + MYSQL_URL + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = '" + MYSQL_USER + "'," +
                "'password' = '" + MYSQL_PASSWORD + "'," +
                "'driver' = '" + DRIVER_CLASS + "'" +
                ")";
    }

    /**
     * 核心修复：精准映射字段类型（与MySQL表结构完全对齐）
     * 参考MySQL建表语句：只将“自增主键ID”设为INT，其余ID（如goods_id/shop_id）均为STRING
     */
    private static String getColumnType(String columnName) {
        // 仅以下字段是MySQL中的自增INT类型（对应建表语句中的AUTO_INCREMENT或INT）
        if (columnName.equals("order_id") ||  // ods_trade_order的自增主键（INT AUTO_INCREMENT）
                columnName.equals("id") ||        // ods_goods_operation_log/ods_marketing_activity_log/ods_shop_operation_log的自增主键（INT AUTO_INCREMENT）
                columnName.equals("stock_num")) { // 库存数量（INT）
            return "INT";
        }
        // 时间字段（MySQL中为DATETIME，Flink对应TIMESTAMP）
        else if (columnName.contains("time")) {
            return "TIMESTAMP";
        }
        // 金额/价格字段（MySQL中为DECIMAL(10,2)，Flink对应DECIMAL(10,2)）
        else if (columnName.contains("amount") || columnName.contains("price")) {
            return "DECIMAL(10,2)";
        }
        // 布尔型字段（MySQL中为TINYINT(1)，Flink对应BOOLEAN，避免之前的Boolean→Integer转换错误）
        else if (columnName.startsWith("is_")) { // is_follow_shop/is_paid
            return "BOOLEAN";
        }
        // 其余字段（如user_id/device_id/goods_id/shop_id等）均为VARCHAR，对应Flink的STRING
        else {
            return "STRING";
        }
    }

    /**
     * Kafka配置（增加重试和超时，提升稳定性）
     */
    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
        properties.setProperty("retries", "3"); // 重试3次
        properties.setProperty("batch.size", "16384"); // 批量发送大小（16KB）
        properties.setProperty("linger.ms", "5"); // 延迟5ms发送（合并小批次）
        properties.setProperty("buffer.memory", "33554432"); // 发送缓冲区大小（32MB）
        properties.setProperty("request.timeout.ms", "30000"); // 请求超时时间
        return properties;
    }

    /**
     * Row→JSON转换（处理null值，避免JSON格式异常）
     */
    public static class RowToJsonMapFunction implements MapFunction<Row, String> {
        private final String[] columnNames;

        public RowToJsonMapFunction(String[] columnNames) {
            this.columnNames = columnNames;
        }

        @Override
        public String map(Row row) throws Exception {
            JSONObject json = new JSONObject();
            for (int i = 0; i < row.getArity(); i++) {
                String columnName = columnNames[i];
                Object value = row.getField(i);
                // 处理null值：JSON中用null表示（避免空指针）
                if (value == null) {
                    json.put(columnName, JSONObject.NULL);
                } else {
                    json.put(columnName, value);
                }
            }
            return json.toString();
        }
    }
}
package com.retailersv1;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DwdUser {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String bootstrapServers = "localhost:9092";
        String sourceTopic = "dwd_user_behavior_log";
        String targetTopic = "dwd_user_behavior_log_enriched";

        // 1. 从 Kafka 读取 dwd_user_behavior_log
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId("dim-enrichment-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        SingleOutputStreamOperator<JSONObject> parsedStream = rawStream.map(JSON::parseObject);

        // 2. 异步 I/O 维度扩展（HBase）
        SingleOutputStreamOperator<JSONObject> enrichedStream = AsyncDataStream.unorderedWait(
                parsedStream,
                new HBaseAsyncDimFunction(),
                30,
                TimeUnit.SECONDS,
                100
        );

        // 3. 写回 Kafka enriched topic
        KafkaSink<String> kafkaSink = KafkaUtils.buildKafkaSink(bootstrapServers, targetTopic);

        enrichedStream
                .map(obj -> JSON.toJSONString(obj))
                .sinkTo(kafkaSink);

        env.execute("User Behavior Log Dimension Enrichment");
    }

    /**
     * 异步 I/O 维度查询函数
     * 这里模拟查询 HBase 的维度表，实际使用时需要集成 HBase Async 客户端
     */
    public static class HBaseAsyncDimFunction extends RichAsyncFunction<JSONObject, JSONObject> {

        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            // 提取关键外键
            String skuId = input.getString("sku_id");
            String activityId = input.getString("activity_id");
            String userId = input.getString("uid");
            String deviceId = input.getString("mid");

            // 模拟异步查询 HBase（实际应使用 HBase Async 客户端）
            CompletableFuture
                    .supplyAsync(() -> {
                        // 这里替换为 HBase 查询逻辑
                        JSONObject dimInfo = new JSONObject();
                        if (skuId != null) {
                            JSONObject sku = new JSONObject();
                            sku.put("sku_name", "商品_" + skuId);
                            sku.put("category", "分类_" + (Integer.parseInt(skuId) % 10));
                            dimInfo.put("sku_info", sku);
                        }
                        if (activityId != null) {
                            JSONObject act = new JSONObject();
                            act.put("activity_name", "活动_" + activityId);
                            act.put("activity_type", "秒杀");
                            dimInfo.put("activity_info", act);
                        }
                        if (userId != null) {
                            JSONObject user = new JSONObject();
                            user.put("user_name", "用户_" + userId);
                            user.put("user_level", "VIP" + (Integer.parseInt(userId) % 5));
                            dimInfo.put("user_info", user);
                        }
                        if (deviceId != null) {
                            JSONObject dev = new JSONObject();
                            dev.put("device_model", "型号_" + deviceId);
                            dev.put("os_version", input.getString("os"));
                            dimInfo.put("device_info", dev);
                        }
                        // 合并到原始 JSON
                        input.put("dim", dimInfo);
                        return input;
                    })
                    .thenAccept(enriched -> resultFuture.complete(Collections.singleton(enriched)));
        }

        @Override
        public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            // 超时直接返回原始数据
            resultFuture.complete(Collections.singleton(input));
        }
    }

}

package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.AsyncHbaseDimBaseDicFunc;
import com.retailersv1.func.IntervalJoinOrderCommentAndOrderInfoFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CommonGenerateTempLate;
import com.stream.utils.SensitiveWordsUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DbusDBCommentFactData2Kafka {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");

    @SneakyThrows
    public static void  main(String[] args){
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.latest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null) {
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        kafkaCdcDbSource.print("kafkaCdcDbSource -> :");


        env.execute();
    }
}

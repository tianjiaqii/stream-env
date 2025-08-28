package com.label.ods;

import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class DbusLogDataProcessToKafka {
    private static final String Kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {};
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {};
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new FsStateBackend(""));




    }
}

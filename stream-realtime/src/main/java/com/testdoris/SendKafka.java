package com.testdoris;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SendKafka {
    public static void main(String[] args) {
        // 配置Kafka生产者属性
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 创建固定的消息内容
        String fixedMessage = "{\"id\":808537,\"msid\":\"ddd59bf95e824ccdb16e89db30bb168e\",\"datatype\":4610,\"data\":\"AB4HB+kMEycGyHWfAVsnuAAjACYAAKydAGAABgAMAAMAAAAA\",\"vehicleno\":\"粤BML606,\",\"datalen\":36,\"vehiclecolor\":2,\"vec1\":35,\"vec2\":38,\"vec3\":44189,\"encrypy\":0,\"altitude\":6,\"alarm\":{\"stolen\":0,\"oilError\":0,\"vssError\":0,\"inOutArea\":0,\"overSpeed\":0,\"inOutRoute\":0,\"cameraError\":0,\"illegalMove\":0,\"stopTimeout\":0,\"earlyWarning\":0,\"icModuleError\":0,\"emergencyAlarm\":0,\"fatigueDriving\":0,\"ttsModuleError\":0,\"gnssModuleError\":0,\"illegalIgnition\":0,\"rolloverWarning\":0,\"overspeedWarning\":0,\"terminalLcdError\":0,\"collisionRollover\":0,\"laneDepartureError\":0,\"roadDrivingTimeout\":0,\"banOnDrivingWarning\":0,\"driverFatigueMonitor\":0,\"gnssAntennaDisconnect\":0,\"gnssAntennaShortCircuit\":0,\"cumulativeDrivingTimeout\":0,\"terminalMainPowerFailure\":0,\"terminalMainPowerUnderVoltage\":0},\"state\":{\"acc\":0,\"lat\":0,\"lon\":0,\"door\":1,\"oilPath\":0,\"location\":0,\"operation\":0,\"loadRating\":0,\"electricCircuit\":0,\"latLonEncryption\":0,\"laneDepartureWarning\":0,\"forwardCollisionWarning\":0},\"lon\":113.800607,\"lat\":22.75116,\"msgId\":4610,\"direction\":96,\"dateTime\":\"20250730121939\",\"op\":\"r\",\"ds\":\"20250730\",\"ts\":1753849179000}";
        String topic = "test-topic";
        String fixedKey = "fixed-key";

        // 创建Kafka生产者
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // 可以选择发送一次或多次固定消息
            // 这里示例发送5次相同的消息
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, fixedKey, fixedMessage);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("消息发送成功 - 主题: %s, 分区: %d, 偏移量: %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("消息发送失败: " + exception.getMessage());
                    }
                });
            }

            producer.flush();
            System.out.println("所有消息发送完成");
        }
    }
}

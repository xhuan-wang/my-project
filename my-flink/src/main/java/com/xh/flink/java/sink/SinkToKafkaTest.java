package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author: xhuan_wang
 * @Title: SinkToKafkaTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 11:33
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //一、Flink1.13开始的用法
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(String)
                .build();

        stream.map(Event::toString).sinkTo(sink);


        //二、Flink1.12以前的用法
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
//                "",
//                new SimpleStringSchema(),
//                properties,
//                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
//        );
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "my-topic",                  // target topic
                new SimpleStringSchema(),    // serialization schema
                properties                 // producer config
                ); // fault-tolerance

        stream.map(Event::toString).addSink(myProducer);

        env.execute();
    }
}

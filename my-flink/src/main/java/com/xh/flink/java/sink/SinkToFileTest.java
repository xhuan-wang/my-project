package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author: xhuan_wang
 * @Title: SinkToFileTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 10:58
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=100", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("D:/IdeaProjects/my-project/my-flink/output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .build();

        //将Event转成string写入文件
        stream.map(Event::toString).addSink(fileSink);

        env.execute();
    }
}

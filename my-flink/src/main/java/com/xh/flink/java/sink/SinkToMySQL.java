package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: SinkToMySQL
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 16:37
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (?, ?)",
                (statement, r) -> {
                    statement.setString(1, r.user);
                    statement.setString(2, r.url);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("")
                        .withDriverName("")
                        .withUsername("")
                        .withPassword("")
                        .build()
        ));

        env.execute();
    }
}

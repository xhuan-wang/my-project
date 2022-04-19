package com.xh.flink.java.sink;

import com.xh.flink.java.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * @author: xhuan_wang
 * @Title: SinkToRedisTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 14:27
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建一个到Redis连接的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("").build();

        env.addSource(new ClickSource())
                .addSink(new RedisSink<>(conf, new MyRedisMapper()));

        env.execute();
    }
}

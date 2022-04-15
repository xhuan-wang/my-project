package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransKeyByTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-15 10:28
 */
public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //1. 使用Lambda表达式：第一个数据类型是keyBy算子的输入类型，第二个数据类型是指定的key的数据类型
        KeyedStream<Event, String> keyByResult1 = stream.keyBy(e -> e.user);

        //2. 使用匿名内部类实现：第一个数据类型是keyBy算子的输入类型，第二个数据类型是指定的key的数据类型
        KeyedStream<Event, String> keyByResult2 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });

        env.execute();
    }
}

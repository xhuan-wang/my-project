package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransPojoAggregationTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-18 9:06
 */
public class TransPojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.keyBy(e -> e.user).max("timestamp").print(); //指定字段名称

        env.execute();
    }
}

package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: xhuan_wang
 * @Title: TransFlatmapTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-15 9:55
 */
public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap());

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String>{
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")){
                out.collect(value.user);
            } else if (value.user.equals("Bob")){
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}

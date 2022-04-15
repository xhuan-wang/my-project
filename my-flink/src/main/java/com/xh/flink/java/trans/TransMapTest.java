package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransMapTest
 * @ProjectName: my-project
 * @Description: Flink算子：Map
 * @date: 2022-04-13 14:47
 */
public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //1. 传入匿名类，实现MapFunction
        SingleOutputStreamOperator<String> mapResult1 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        //2. 传入MapFunction的实现类
        SingleOutputStreamOperator<String> mapResult2 = stream.map(new UserExtractor());

        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String>{
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}

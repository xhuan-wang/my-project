package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransFileterTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-15 9:32
 */
public class TransFileterTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //1. 传入匿名类实现FilterFunction
        SingleOutputStreamOperator<Event> filterResult1 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        });

        //2. 传入FilterFunction实现类
        SingleOutputStreamOperator<Event> filterResult2 = stream.filter(new UserFilter());
    }

    public static class UserFilter implements FilterFunction<Event>{
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}

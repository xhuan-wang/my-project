package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransFunctionUDFTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-18 11:12
 */
public class TransFunctionUDFTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        clicks.filter(new FlinkFilter());
    }

    public static class FlinkFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains("home");
        }
    }
}

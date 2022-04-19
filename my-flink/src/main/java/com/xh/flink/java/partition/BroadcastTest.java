package com.xh.flink.java.partition;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: BroadcastTest
 * @ProjectName: my-project
 * @Description:
 * 广播：
 * 这种方式其实不应该叫做“重分区”，因为经过广播后，数据会在不同的分区都保留一份，可能进行重复处理。
 * @date: 2022-04-19 10:26
 */
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //经广播后打印输出，并行度为4
        stream.broadcast().print("broadcast").setParallelism(4);

        env.execute();
    }
}

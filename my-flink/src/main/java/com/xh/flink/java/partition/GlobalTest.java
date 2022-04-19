package com.xh.flink.java.partition;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: GlobalTest
 * @ProjectName: my-project
 * @Description:
 * 全局分区：
 * 全局分区也是一种特殊的分区方式，这种做法非常极端，通过调用global()方法，
 * 将所有的输入流数据都发送到下游算子的第一个并行子任务中去，相当于强行让下游
 * 任务并行度成1，这个操作需要非常谨慎，可能对程序造成很大的压力。
 * @date: 2022-04-19 10:31
 */
public class GlobalTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //经广播后打印输出，并行度为4
        stream.global().print("global").setParallelism(4);

        env.execute();
    }
}

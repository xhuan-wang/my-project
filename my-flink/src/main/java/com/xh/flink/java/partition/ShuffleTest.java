package com.xh.flink.java.partition;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: ShuffleTest
 * @ProjectName: my-project
 * @Description:
 * 随机分区(shuffle)：
 * 最简单的重分区就是直接“洗牌”，通过调用DataStream的shuffle()方法，
 * 将数据随机地分配到下游算子的并行任务重去。
 * 镜柜随机分区后，得到的依然是一个DataStream。
 * @date: 2022-04-19 9:53
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //经洗牌后打印输出，并行度为4
        stream.shuffle().print("shuffle").setParallelism(4);

        env.execute();
    }
}

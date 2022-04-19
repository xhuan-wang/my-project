package com.xh.flink.java.partition;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: RebalanceTest
 * @ProjectName: my-project
 * @Description:
 * 轮询分区：
 * 轮询也是一种常见的分区方式，简单来说就是“发牌”，按照先后顺序将数据一次分发。
 * 通过调用DataStream的rebalance()方法就可以实现轮询重分区。
 * rebalance使用的是Round-Robin负载均衡算法，将输入流数据平均分配到下游的并行任务中去。
 * Round-Robin负载均衡算法在很多地方使用，比如Kafka和Nginx
 * @date: 2022-04-19 9:58
 */
public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //经轮询重分区后打印输出，并行度为4
        stream.rebalance().print("rebalance").setParallelism(4);

        env.execute();
    }
}

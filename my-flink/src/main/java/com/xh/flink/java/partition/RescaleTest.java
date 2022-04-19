package com.xh.flink.java.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author: xhuan_wang
 * @Title: RescaleTest
 * @ProjectName: my-project
 * @Description:
 * 重缩放分区：
 * 重缩放分区和轮询分区非常相似，当调用rescale()方法时，其实底层也是使用Round-Robin算法进行轮询的。
 * 但是只会将数据发送到下游并行任务的一部分。也就是说，“发牌人”如果有多个，那么rebalance的方式是每个发牌人都
 * 面向所有人发牌，而rescale的做法是分成小团体，发牌人只给自己的团体内的所有人轮流发牌。
 * 当下游任务的数据量是上游任务的整数倍时，rescale的效率明显会更高。
 * 由于rebalance是所有分区数据的“重新平衡”，当TaskManager数据量较多时，跨节点网络传输必然影响效率；
 * 而如果配置task slot数量合适，用rescale的方式进行“局部重缩放”，就可以让数据只在当前TaskManager的多个slot
 * 之间重新分配，从而避免网络传输带来的损耗。
 * @date: 2022-04-19 10:12
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //这里使用了并行数据源的富函数版本
        //这样可以调用getRuntimeContext方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 8; i++){
                    //将奇数发送到索引为1的并行子任务
                    //将偶数发送到索引为0的并行子任务
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i + 1);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        })
        .setParallelism(2)
        //可以将rescale换成rebalance看看两者的区别
        .rescale()
        .print()
        .setParallelism(4);

        env.execute();
    }
}

package com.xh.flink.java.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: CustomPartitionTest
 * @ProjectName: my-project
 * @Description:
 * 自定义分区：
 * 通过使用partitionCustom()方法来自定义分区策略
 * 在调用时，方法需要传入 两个参数，第一个是自定义分区器(Partitioner)对象，
 * 第二个是应用分区器的字段，他的指定方式与KeyBy指定key基本一样，通过索引来指定或者字段名来指定。
 * @date: 2022-04-19 10:36
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);

        env.execute();
    }
}

package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransReduceTest
 * @ProjectName: my-project
 * @Description:
 * 我们将数据流按照用户id进行分区，然后用一个 reduce算子实现 sum的功能，统计每个
 * 用户访问的频次；进而将所有统计结果分到一组，用另一个 reduce算子实现 maxBy的功能，
 * 记录所有用户中访问频次最高的那个，也就是当前访问量最大的用户是谁。
 * @date: 2022-04-18 10:20
 */
public class TransReduceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                //将Event数据类型转换成元祖类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                //使用用户名进行分流
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //每到一条数据，用户pv的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                //为每一条数据分配一个key，将聚合结果发送到一条流中去
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();

        env.execute();
    }
}

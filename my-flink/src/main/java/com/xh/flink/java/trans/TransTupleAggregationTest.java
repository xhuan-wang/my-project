package com.xh.flink.java.trans;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: TransTupleAggreationTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-15 10:54
 */
public class TransTupleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult1 = stream.keyBy(r -> r.f0).sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult2 = stream.keyBy(r -> r.f0).sum("f1");
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxResult1 = stream.keyBy(r -> r.f0).max(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxResult2 = stream.keyBy(r -> r.f0).max("f1");
        SingleOutputStreamOperator<Tuple2<String, Integer>> minResult1 = stream.keyBy(r -> r.f0).min(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> minResult2 = stream.keyBy(r -> r.f0).min("f1");
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxByResult1 = stream.keyBy(r -> r.f0).maxBy(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxByResult2 = stream.keyBy(r -> r.f0).maxBy("f1");
        SingleOutputStreamOperator<Tuple2<String, Integer>> minByResult1 = stream.keyBy(r -> r.f0).minBy(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> minByResult2 = stream.keyBy(r -> r.f0).minBy("f1");
        minByResult2.print();
        env.execute();
    }
}

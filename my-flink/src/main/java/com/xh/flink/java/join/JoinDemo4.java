package com.xh.flink.java.join;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: xhuan_wang
 * @descrition:
 * 维表join方式：
 * 3、广播维表：
 * 利用Flink的Broadcast State将维度数据流广播到下游做join操作。
 * 特点：
 * 1、优点：维度数据变更后可以即时更新到结果中。
 * 2、缺点：数据保存在内存中，支持的维度数据量比较小。
 *
 * 可以使用缓存来存储一部分常访问的维表数据，以减少访问外部系统的次数，比如使用guava Cache。
 * 需求是：一个主流中数据是用户信息，字段包括用户姓名、城市id；维表是城市数据，字段包括城市ID、城市名称。
 * 要求用户表与城市表关联，输出为：用户名称、城市ID、城市名称
 * @date 2022-05-11 00:20
 */
public class JoinDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //定义主流
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        //定义城市流
        DataStream<Tuple2<Integer, String>> cityStream = env.socketTextStream("localhost", 9001, "\n")
                .map(p -> {
                    //输入格式为：城市ID,城市名称
                    String[] list = p.split(",");
                    return new Tuple2<Integer, String>(Integer.valueOf(list[0]), list[1]);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {
                });

        //将城市流定义为广播流
        final MapStateDescriptor<Integer, String> broadcastDesc = new MapStateDescriptor("broad1", Integer.class, String.class);
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = cityStream.broadcast(broadcastDesc);

        DataStream result = textStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<Integer, String>, Tuple3<String, Integer, String>>() {
                    //处理非广播流，关联维度
                    @Override
                    public void processElement(Tuple2<String, Integer> value, ReadOnlyContext ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        ReadOnlyBroadcastState<Integer, String> state = ctx.getBroadcastState(broadcastDesc);
                        String cityName = "";
                        if (state.contains(value.f1)) {
                            cityName = state.get(value.f1);
                        }
                        out.collect(new Tuple3<>(value.f0, value.f1, cityName));
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        System.out.println("收到广播数据：" + value);
                        ctx.getBroadcastState(broadcastDesc).put(value.f0, value.f1);
                    }
                });


        result.print();
        env.execute("joinDemo");
    }
}

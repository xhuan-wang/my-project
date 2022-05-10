package com.xh.flink.java.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: xhuan_wang
 * @descrition:
 * 维表join方式：
 * 4、Temporal table function join：
 * Temporal table是持续变化表上某一时刻的视图，Temporal table function是一个表函数，
 * 传递一个时间参数，返回Temporal table这一指定时刻的视图。
 * 可以将维度数据流映射为Temporal table，主流与这个Temporal table进行关联，可以关联到某一个版本（历史上某一个时刻）的维度数据。
 * 特点：
 * 1、优点：维度数据量可以很大，维度数据更新及时，不依赖外部存储，可以关联不同版本的维度数据。
 * 2、缺点：只支持在Flink SQL API中使用。
 *
 * EventTime的一个实例
 *
 * 可以使用缓存来存储一部分常访问的维表数据，以减少访问外部系统的次数，比如使用guava Cache。
 * 需求是：一个主流中数据是用户信息，字段包括用户姓名、城市id；维表是城市数据，字段包括城市ID、城市名称。
 * 要求用户表与城市表关联，输出为：用户名称、城市ID、城市名称
 * @date 2022-05-11 00:20
 */
public class JoinDemo6 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定是EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);

        //主流，用户流, 格式为：user_name、city_id、ts
        List<Tuple3<String, Integer, Long>> list1 = new ArrayList<>();
        list1.add(new Tuple3<>("user1", 1001, 1L));
        list1.add(new Tuple3<>("user1", 1001, 10L));
        list1.add(new Tuple3<>("user2", 1002, 2L));
        list1.add(new Tuple3<>("user2", 1002, 15L));
        DataStream<Tuple3<String, Integer, Long>> textStream = env.fromCollection(list1)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                                return element.f2;
                            }
                        }
                );

        //定义城市流,格式为：city_id、city_name、ts
        List<Tuple3<Integer, String, Long>> list2 = new ArrayList<>();
        list2.add(new Tuple3<>(1001, "beijing", 1L));
        list2.add(new Tuple3<>(1001, "beijing2", 10L));
        list2.add(new Tuple3<>(1002, "shanghai", 1L));
        list2.add(new Tuple3<>(1002, "shanghai2", 5L));

        DataStream<Tuple3<Integer, String, Long>> cityStream = env.fromCollection(list2)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, String, Long>>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Tuple3<Integer, String, Long> element) {
                                return element.f2;
                            }
                        });

        //转变为table
        Table userTable = tableEnv.fromDataStream(textStream, "user_name,city_id,ts.rowtime");
        Table cityTable = tableEnv.fromDataStream(cityStream, "city_id,city_name,ts.rowtime");

        tableEnv.createTemporaryView("userTable", userTable);
        tableEnv.createTemporaryView("cityTable", cityTable);

        //定义一个TemporalTableFunction
        TemporalTableFunction dimCity = cityTable.createTemporalTableFunction("ts", "city_id");
        //注册表函数
        tableEnv.registerFunction("dimCity", dimCity);

        //关联查询
        Table result = tableEnv
                .sqlQuery("select u.user_name,u.city_id,d.city_name,u.ts from userTable as u " +
                        ", Lateral table (dimCity(u.ts)) d " +
                        "where u.city_id=d.city_id");

        //打印输出
        DataStream resultDs = tableEnv.toAppendStream(result, Row.class);
        resultDs.print();
        env.execute("joinDemo");
    }
}

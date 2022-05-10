package com.xh.flink.java.join;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author: xhuan_wang
 * @descrition:
 * 维表join方式：
 * 2、热存储维表：
 * 将维表数据存储在Redis、HBase、MySQL等外部存储中，实时流在关联维表数据的时候实时去外部存储中查询
 * 特点：
 * 1、优点：维度数据量不受内存限制，可以存储很大的数据量。
 * 2、缺点：因为维表数据在外部存储中，读取速度受制于外部存储的读取速度；另外维表的同步也有延迟。
 *
 * (2) 使用异步IO来提高访问吞吐量：
 * 可以使用缓存来存储一部分常访问的维表数据，以减少访问外部系统的次数，比如使用guava Cache。
 * 需求是：一个主流中数据是用户信息，字段包括用户姓名、城市id；维表是城市数据，字段包括城市ID、城市名称。
 * 要求用户表与城市表关联，输出为：用户名称、城市ID、城市名称
 * @date 2022-05-11 00:20
 */
public class JoinDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });


        DataStream<Tuple3<String,Integer, String>> orderedResult = AsyncDataStream
                //保证顺序：异步返回的结果保证顺序，超时时间1秒，最大容量2，超出容量触发反压
                .orderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        DataStream<Tuple3<String,Integer, String>> unorderedResult = AsyncDataStream
                //允许乱序：异步返回的结果允许乱序，超时时间1秒，最大容量2，超出容量触发反压
                .unorderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        orderedResult.print();
        unorderedResult.print();
        env.execute("joinDemo3");
    }

    //定义个类，继承RichAsyncFunction，实现异步查询存储在mysql里的维表
    //输入用户名、城市ID，返回 Tuple3<用户名、城市ID，城市名称>
    static class JoinDemo3AyncFunction extends RichAsyncFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {
        // 链接
        private static String jdbcUrl = "jdbc:mysql://192.168.145.1:3306?useSSL=false";
        private static String username = "root";
        private static String password = "123";
        private static String driverName = "com.mysql.jdbc.Driver";
        java.sql.Connection conn;
        PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            ps = conn.prepareStatement("select city_name from tmp.city_info where id = ?");
        }

        @Override
        public void close() throws Exception {
            super.close();
            conn.close();
        }

        //异步查询方法
        @Override
        public void asyncInvoke(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            // 使用 city id 查询
            ps.setInt(1, input.f1);
            ResultSet rs = ps.executeQuery();
            String cityName = null;
            if (rs.next()) {
                cityName = rs.getString(1);
            }
            List list = new ArrayList<Tuple2<Integer, String>>();
            list.add(new Tuple3<>(input.f0,input.f1, cityName));
            resultFuture.complete(list);
        }

        //超时处理
        @Override
        public void timeout(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            List list = new ArrayList<Tuple2<Integer, String>>();
            list.add(new Tuple3<>(input.f0,input.f1, ""));
            resultFuture.complete(list);
        }
    }
}

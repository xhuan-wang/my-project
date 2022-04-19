package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xhuan_wang
 * @Title: RichFunctionTest
 * @ProjectName: my-project
 * @Description:
 * Flink富函数一个比较常用的场景，如果希望连接一个外部数据库进行读写操作，
 * 那么将连接的操作放在map()方法中显然不是一个好的选择——因为来一条数据建立一个连接；
 * 所以可以在open()方法中建立连接，在map()中读写数据，在close()中关闭连接，最佳实践：
 * public class myFlatMap extends RichFlatMapFunction<IN, OUT>{
 * @Override
 * public void open(Configuration configuration) {
 *     //做初始化工作
 *     //例如建立一个和MySQL的连接
 * }
 *
 * @Override
 * public void flatMap(IN in, Collector<OUT out){
 *     //对数据库进行读写
 * }
 * @Override
 * public void close(){
 *     //清理工作，关闭和MySQL数据库的连接
 * }
 * }
 * @date: 2022-04-18 11:30
 */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=4", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        //将点击事件转换成长整型的时间戳输出
        clicks.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为 : " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为 : " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }

            @Override
            public Long map(Event value) throws Exception {
                return value.timestamp;
            }
        }).print();

        env.execute();
    }
}

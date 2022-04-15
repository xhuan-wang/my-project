package com.xh.flink.java.source;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author: xhuan_wang
 * @Title: ClickSource
 * @ProjectName: my-project
 * @Description: 通过SourceFunction接口自定义数据源，
 * 注意：继承SourceFunction接口实现的数据源的并行度只能设为1，
 *      如果需要设置大于1，则需要实现ParallelSourceFunction接口
 * @date: 2022-04-13 9:34
 */
public class ClickSource implements SourceFunction<Event> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running){
            ctx.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new ClickSource()).print();
        env.execute();
    }
}

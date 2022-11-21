package com.xh.flink.java.process;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: xhuan_wang
 * @Title: ProcessingTimeTimerTest
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-24 16:59
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //处理时间语义，不需要分配时间戳和watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
        stream.print();
        stream.keyBy(event -> {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    return dateFormat.format(new Date(Long.valueOf(event.timestamp)));
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
//                .evictor()
                .aggregate(new CountAgg(), new WindowResult())
//                .keyBy(result -> result.f0)
//                .process()

        ;

        //要用定时器，必须给于keyedStream
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timerService().currentProcessingTime();

                        out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                        //注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                })
                /*.print()*/;

        env.execute();
    }

    /***
     * @description TODO
     * AggregateFunction<IN,ACC,OUT>
     * @return
     * @author xiaohuan
     * @date 2022/11/21 11:16:51
     */
    public static class CountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    /***
     * @description TODO
     * ProcessWindowFunction<IN, OUT, KEY, W extends Window>
     * @return
     * @author xiaohuan
     * @date 2022/11/21 11:18:10
     */
    public static class WindowResult extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>.Context ctx, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String start = dateFormat.format(new Date(Long.valueOf(ctx.window().getStart())));
            String end = dateFormat.format(new Date(Long.valueOf(ctx.window().getEnd())));
            System.out.println(start + " - " + end);
            collector.collect(Tuple2.of(key, iterable.iterator().next()));
        }
    }
}

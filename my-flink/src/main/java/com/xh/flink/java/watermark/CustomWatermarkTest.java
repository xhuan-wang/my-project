package com.xh.flink.java.watermark;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author: xhuan_wang
 * @Title: CustomWatermarkTest
 * @ProjectName: my-project
 * @Description: 自定义水位线策略
 * @date: 2022-04-24 9:24
 */
public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>(){

                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp; //告诉程序数据源里的时间戳是哪个字段
                }
            };
        }
    }

    /**
     * 1、自定义周期性水位线生成器
     */
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; //延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; //观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); //更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * 2、断点式水位线生成器
     */
    public class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //只有在遇到特定的itemId时，才发出水位线
            if (event.user.equals("Mary")){
                output.emitWatermark(new Watermark(event.timestamp -1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //不需要做任何事情，因为在onEvent方法中发射水位线了
        }
    }
}

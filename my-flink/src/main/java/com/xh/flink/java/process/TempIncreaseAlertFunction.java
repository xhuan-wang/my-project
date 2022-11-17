package com.xh.flink.java.process;

import com.xh.flink.java.pojo.SensorReading;
import com.xh.flink.java.source.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Classname TempIncreaseAlertFunction
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 16:32
 * @Created by xiaohuan
 */
public class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {
    private ValueState<Double> lastTemp;
    private ValueState<Long> currentTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastTemp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-temp", Types.DOUBLE)
        );
        currentTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("current-timer", Types.LONG)
        );
    }

    @Override
    public void processElement(SensorReading r, Context ctx, Collector<String> out) throws Exception {
        // 取出上一次的温度
        Double prevTemp = 0.0;
        if (lastTemp.value() != null) {
            prevTemp = lastTemp.value();
        }
        // 将当前温度更新到上一次的温度这个变量中
        lastTemp.update(r.temperature);

        Long curTimerTimestamp = 0L;
        if (currentTimer.value() != null) {
            curTimerTimestamp = currentTimer.value();
        }
        if (prevTemp == 0.0 || r.temperature < prevTemp) {
            // 温度下降或者是第一个温度值，删除定时器
            ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
            // 清空状态变量
            currentTimer.clear();
        } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
            // 温度上升且我们并没有设置定时器
            long timerTs = ctx.timerService().currentProcessingTime() + 1000L;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
            // 保存定时器时间戳
            currentTimer.update(timerTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        out.collect("传感器id为: "
                + ctx.getCurrentKey()
                + "的传感器温度值已经连续1s上升了。");
        currentTimer.clear();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource()).returns(SensorReading.class);

        DataStream<String> warings = readings
                .keyBy(r -> r.id)
                .process(new TempIncreaseAlertFunction());

        warings.print();

        env.execute("Multi-Stream Transformations Example");
    }
}

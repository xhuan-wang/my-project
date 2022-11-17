package com.xh.flink.java.process;

import com.xh.flink.java.pojo.SensorReading;
import com.xh.flink.java.source.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Classname SensorSwitch
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 16:37
 * @Created by xiaohuan
 */
public class SensorSwitch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource()).returns(SensorReading.class);

        KeyedStream<SensorReading, String> keyStream = stream
                .keyBy(r -> r.id);

        KeyedStream<Tuple2<String, Long>, String> switches = env
                .fromElements(Tuple2.of("sensor_2", 10 * 1000L))
                .keyBy(r -> r.f0);

        keyStream
                .connect(switches)
                .process(new SwitchProcess())
                .print();

        env.execute();
    }

    public static class SwitchProcess extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        private ValueState<Boolean> forwardingEnabled;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            forwardingEnabled = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN)
            );
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            forwardingEnabled.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            forwardingEnabled.clear();
        }
    }
}

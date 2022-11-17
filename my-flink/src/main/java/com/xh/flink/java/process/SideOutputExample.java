package com.xh.flink.java.process;

import com.xh.flink.java.pojo.SensorReading;
import com.xh.flink.java.source.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Classname SideOutputExample
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 16:35
 * @Created by xiaohuan
 */
public class SideOutputExample {

    private static OutputTag<String> output = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource()).returns(SensorReading.class);

        SingleOutputStreamOperator<SensorReading> warnings = stream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.temperature < 32) {
                            ctx.output(output, "温度小于32度！");
                        }
                        out.collect(value);
                    }
                });

        warnings.print();
        warnings.getSideOutput(output).print();

        env.execute();
    }
}

package com.xh.flink.java.trans;

import com.xh.flink.java.pojo.Alert;
import com.xh.flink.java.pojo.SensorReading;
import com.xh.flink.java.pojo.SmokeLevel;
import com.xh.flink.java.source.SensorSource;
import com.xh.flink.java.source.SmokeLevelSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Classname MultiStreamTransformations
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 15:54
 * @Created by xiaohuan
 */
public class MultiStreamTransformations {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1、union算子
        DataStream<SensorReading> parisStream = env
                .addSource(new SensorSource()).returns(SensorReading.class);
        DataStream<SensorReading> tokyoStream = env
                .addSource(new SensorSource()).returns(SensorReading.class);
        DataStream<SensorReading> rioStream = env
                .addSource(new SensorSource()).returns(SensorReading.class);
        DataStream<SensorReading> allCities = parisStream
                .union(tokyoStream, rioStream);

//        allCities.print();

        //2、connect算子
        DataStream<SensorReading> tempReadings = env
                .addSource(new SensorSource()).returns(SensorReading.class);

        DataStream<SmokeLevel> smokeReadings = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        ConnectedStreams<SensorReading, SmokeLevel> connected = tempReadings.connect(smokeReadings);

        KeyedStream<SensorReading, String> keyedTempReadings = tempReadings
                .keyBy(r -> r.id);

        DataStream<Alert> alerts = keyedTempReadings
                .connect(smokeReadings.broadcast())
                .flatMap(new RaiseAlertFlatMap());

//        alerts.print();

        env.execute("Multi-Stream Transformations Example");
    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading tempReading, Collector<Alert> out) throws Exception {
            // high chance of fire => true
            if (this.smokeLevel == SmokeLevel.HIGH && tempReading.temperature > 100) {
                out.collect(new Alert("Risk of fire! " + tempReading, tempReading.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> out) {
            // update smoke level
            this.smokeLevel = smokeLevel;
        }
    }
}

package com.xh.flink.java.source;

import com.xh.flink.java.pojo.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Classname SensorSource
 * @Description 自定义数据源
 * @Version 1.0.0
 * @Date 2022/11/17 15:46
 * @Created by xiaohuan
 */
public class SensorSource extends RichParallelSourceFunction {

    private Boolean running = true;

    @Override
    public void run(SourceContext ctx) throws Exception {

        Random rand = new Random();

        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i;
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                ctx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}

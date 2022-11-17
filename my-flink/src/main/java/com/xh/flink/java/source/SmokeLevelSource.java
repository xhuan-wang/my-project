package com.xh.flink.java.source;

import com.xh.flink.java.pojo.SmokeLevel;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Classname SmokeLevelSource
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 15:53
 * @Created by xiaohuan
 */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> srcCtx) throws Exception {

        Random rand = new Random();

        while (running) {

            if (rand.nextGaussian() > 0.8) {
                srcCtx.collect(SmokeLevel.HIGH);
            } else {
                srcCtx.collect(SmokeLevel.LOW);
            }

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
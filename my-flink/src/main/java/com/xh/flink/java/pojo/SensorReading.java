package com.xh.flink.java.pojo;

/**
 * @Classname SensorReading
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 15:50
 * @Created by xiaohuan
 */
public class SensorReading {

    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}

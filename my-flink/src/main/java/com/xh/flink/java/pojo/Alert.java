package com.xh.flink.java.pojo;

/**
 * @Classname Alert
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/17 15:51
 * @Created by xiaohuan
 */
public class Alert {

    public String message;
    public long timestamp;

    public Alert() { }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "(" + message + ", " + timestamp + ")";
    }
}

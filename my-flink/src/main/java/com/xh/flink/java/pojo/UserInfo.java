package com.xh.flink.java.pojo;

import java.io.Serializable;

/**
 * @author: xhuan_wang
 * @descrition:
 * @date 2022-05-11 00:58
 */
public class UserInfo implements Serializable {
    private String userName;
    private Integer cityId;
    private Long ts;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}

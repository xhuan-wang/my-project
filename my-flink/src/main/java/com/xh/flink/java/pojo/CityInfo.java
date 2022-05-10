package com.xh.flink.java.pojo;

import java.io.Serializable;

/**
 * @author: xhuan_wang
 * @descrition:
 * @date 2022-05-11 00:59
 */
public class CityInfo implements Serializable {
    private Integer cityId;
    private String cityName;
    private Long ts;

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}

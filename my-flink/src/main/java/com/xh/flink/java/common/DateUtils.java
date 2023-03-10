package com.xh.flink.java.common;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @Classname DateUtils
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/9/26 9:48
 * @Created by xiaohuan
 */
public class DateUtils {

    /**
     * 获取当天的零点时间戳
     *
     * @return 当天的零点时间戳
     */
    public static long getTodayStartTime() {
        //设置时区
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTimeInMillis();
    }

    /**
     * @description 获取当前日期，格式：yyyy-MM-dd
     *
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 15:08:37
     */
    public static String getCurrentDate(){
        return DateUtil.today();
    }

    /**
     * @description 获取当前时间，格式：yyyy-MM-dd HH:mm:ss
     *
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 15:48:17
     */
    public static String getCurrentDateTime(){
        return DateUtil.now();
    }

    /***
     * @description 获取当前日期间隔指定天数的日期
     * @param diff
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 15:43:51
     */
    public static String getCurrentDate(int diff){
        Date date = DateUtil.parse(DateUtil.today());
        return DateUtil.format(DateUtil.offsetDay(date, diff), "yyyy-MM-dd");
    }

    public static String getCurrentDateTime(int diff){
        Date date = DateUtil.parse(DateUtil.now());
        return DateUtil.format(DateUtil.offsetDay(date, diff), "yyyy-MM-dd HH:mm:ss");
    }

    /***
     * @description 根据指定日期指定格式返回指定时间间隔的日期
     * @param currDate
     * @param diff 0-当前
     * @param format 格式
     * @return 获取指定日期偏移指定天的日期
     * @author xiaohuan
     * @date 2022/11/22 11:59:51
     */
    public static String getDateAdd(String currDate, int diff, String format) {
        Date date = DateUtil.parse(currDate);
        return DateUtil.format(DateUtil.offsetDay(date, diff), format);
    }

    /***
     * @description 获取某一天的开始时间， 如：2017-03-15 22:33:23，获得2017-03-15 00:00:00
     * @param dateStr
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 15:56:42
     */
    public static String getBeginOfDay(String dateStr){
        Date date = DateUtil.parse(dateStr);
        return DateUtil.format(DateUtil.beginOfDay(date), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * @description 获取某一天的结束时间，比如： 2017-03-15 22:33:23，获得2017-03-15 23:59:59
     * @param dateStr
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 15:58:05
     */
    public static String getEndOfDay(String dateStr){
        Date date = DateUtil.parse(dateStr);
        return DateUtil.format(DateUtil.endOfDay(date), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * @description 两个时间天数差
     * @param beginDate 开始时间
     * @param endDate 结束时间
     * @return java.lang.Long 天数
     * @author xiaohuan
     * @date 2022/11/22 15:53:52
     */
    public static Long dayBetween(String beginDate, String endDate) {
        Date date1 = DateUtil.parse(beginDate);
        Date date2 = DateUtil.parse(endDate);
        return DateUtil.between(date1, date2, DateUnit.DAY);
    }

    /**
     * @description 根据指定日期格式，格式化指定日期
     * 如：getFormatDate("2017-03-15 22:33:23", "yyyyMM")，返回 201703
     * @param dateStr
     * @param format
     * @return java.lang.String
     * @author xiaohuan
     * @date 2022/11/22 16:02:24
     */
    public static String getFormatDate(String dateStr, String format){
        Date date = DateUtil.parse(dateStr);
        return DateUtil.format(date, format);
    }

    public static void main(String[] args) {
//        System.out.println(DateUtils.dayBetween(DateUtils.getFormatDate("2022-12-07 23:18:42", "yyyy-MM-dd"), DateUtils.getCurrentDate()));
        System.out.println(DateUtils.getCurrentDate(-360));
    }
}

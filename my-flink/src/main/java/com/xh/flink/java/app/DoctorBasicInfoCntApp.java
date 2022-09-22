package com.xh.flink.java.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xh.flink.java.common.ApplicationPropertiesConfig;
import com.xh.flink.java.pojo.DoctorBasicInfo;
import com.xh.flink.java.sink.MySQLJDBCSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * @Classname DoctorBasicInfoCntApp
 * @Description 医生基本信息统计
 * @Version 1.0.0
 * @Date 2022/9/21 17:25
 * @Created by xiaohuan
 */
@Slf4j
public class DoctorBasicInfoCntApp {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ApplicationPropertiesConfig propertiesConfig = new ApplicationPropertiesConfig();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(propertiesConfig.getProperty("db.ip"))
                .port(Integer.parseInt(propertiesConfig.getProperty("db.port")))
                .databaseList(propertiesConfig.getProperty("db.databaseName"))
                .tableList(propertiesConfig.getProperty("db.databaseName") + "." + propertiesConfig.getProperty("db.doctor_basic_info"))
                .username(propertiesConfig.getProperty("db.username"))
                .password(propertiesConfig.getProperty("db.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将SourceRecord 转成 JSON 字符串，格式如下注释：
                .build();

        DataStreamSource<String> sourceData = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "doctor_basic_info");

        DataStream<DoctorBasicInfo> filterData =
                sourceData.filter(new FilterFunction<String>() {
                            @Override
                            public boolean filter(String line) throws Exception {
                                JSONObject record = JSON.parseObject(line);
                                return record.get("before") == null;
                            }
                        }).uid("FilterInsert")
                        .map(line -> {
                            JSONObject record = JSON.parseObject(line);
                            DoctorBasicInfo doctorBasicInfo = null;
                            if (record.get("before") == null){
                                try {
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
                                    doctorBasicInfo = JSON.parseObject(record.get("after").toString(), DoctorBasicInfo.class);
                                    doctorBasicInfo.setPracRecDate(doctorBasicInfo.getPracRecDate()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getPracRecDate()) - 28800000)) : null);
                                    doctorBasicInfo.setCertRecDate(doctorBasicInfo.getCertRecDate()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getCertRecDate()) - 28800000)) : null);
                                    doctorBasicInfo.setTitleRecDate(doctorBasicInfo.getTitleRecDate()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getTitleRecDate()) - 28800000)) : null);
                                    doctorBasicInfo.setDocMultiSitedDateStart(doctorBasicInfo.getDocMultiSitedDateStart()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getDocMultiSitedDateStart()) - 28800000)) : null);
                                    doctorBasicInfo.setDocMultiSitedDateEnd(doctorBasicInfo.getDocMultiSitedDateEnd()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getDocMultiSitedDateEnd()) - 28800000)) : null);
                                    doctorBasicInfo.setHosOpinionDate(doctorBasicInfo.getHosOpinionDate()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getHosOpinionDate()) - 28800000)) : null);
                                    doctorBasicInfo.setDocBirthdate(doctorBasicInfo.getDocBirthdate()!=null? dateFormat1.format(new Date(Long.valueOf(doctorBasicInfo.getDocBirthdate()) - 28800000)) : null);
                                    doctorBasicInfo.setUpdatedLast(doctorBasicInfo.getUpdatedLast()!=null?new Timestamp(doctorBasicInfo.getUpdatedLast().getTime() - 28800000) : null);
                                    doctorBasicInfo.setUpdateDate(doctorBasicInfo.getUpdateDate()!=null? dateFormat.format(new Date(Long.valueOf(doctorBasicInfo.getUpdateDate()) - 28800000)) : null);
                                }catch (Exception e){
                                    log.error("解析错误：" + line);
                                    e.printStackTrace();
                                }
                            }
                            return doctorBasicInfo;
                        }).uid("MapConvertsJSONToObject")
                        /*.filter(new FilterFunction<DoctorBasicInfo>() {
                            @Override
                            public boolean filter(DoctorBasicInfo doctorBasicInfo) throws Exception {
                                String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                                return doctorBasicInfo.getHosOpinionDate().substring(0, 10).equals(currentDate);
                            }
                        }).uid("FilterCurrentDateData")*/;

        DataStream<Tuple2<String, Long>> countAllDoctorResult =
                filterData.keyBy(new KeySelector<DoctorBasicInfo, String>() {
                    @Override
                    public String getKey(DoctorBasicInfo doctorBasicInfo) throws Exception {
                        return doctorBasicInfo.getHosOpinionDate().substring(0, 10);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .aggregate(new CountAggDoctor(), new windowFunctionDoctorAll());

        DataStream<Tuple3<String, String, Long>> countGroupByOrgCodeResult =
                filterData.keyBy(new KeySelector<DoctorBasicInfo, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DoctorBasicInfo doctorBasicInfo) throws Exception {
                        return Tuple2.of(doctorBasicInfo.getHosOpinionDate().substring(0, 10), doctorBasicInfo.getOrgCode());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .aggregate(new CountAggDoctorGroupByOrgCode(), new windowFunctionDoctorGroupByOrgCode());

        SinkFunction<Tuple2<String, Long>> sinkAllDoctorMysql = new MySQLJDBCSink<>(
                "INSERT INTO supervise_stat_cnt_all_df\n" +
                        "(business_date, business_type, target_type, target_name, target_value)\n" +
                        "VALUES(?, ?, ?, ?, ?)\n" +
                        "ON DUPLICATE KEY UPDATE target_value=?"
                ,
                new JdbcStatementBuilder<Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple2<String, Long> result) throws SQLException {
                        ps.setString(1, result.f0);
                        ps.setString(2, "doctor");
                        ps.setString(3, "count");
                        ps.setString(4, "doctor_cnt_1d");
                        ps.setLong(5, result.f1);
                        ps.setLong(6, result.f1);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countAllDoctorResult.addSink(sinkAllDoctorMysql);

        SinkFunction<Tuple3<String, String, Long>> sinkGroupByOrgCodeMysql = new MySQLJDBCSink<>(
                "INSERT INTO organization_stat_cnt_org_code_df\n" +
                        "(business_date, business_type, org_code, target_type, target_name, target_value)\n" +
                        "VALUES(?, ?, ?, ?, ?, ?)\n" +
                        "ON DUPLICATE KEY UPDATE target_value=?"
                ,
                new JdbcStatementBuilder<Tuple3<String, String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple3<String, String, Long> result) throws SQLException {
                        ps.setString(1, result.f0);
                        ps.setString(2, "doctor");
                        ps.setString(3, result.f1);
                        ps.setString(4, "count");
                        ps.setString(5, "doctor_org_cnt_1d");
                        ps.setLong(6, result.f2);
                        ps.setLong(7, result.f2);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countGroupByOrgCodeResult.addSink(sinkGroupByOrgCodeMysql);

        env.execute("doctor_basic_info" + System.currentTimeMillis());
    }

    public static class windowFunctionDoctorGroupByOrgCode implements WindowFunction<Long, Tuple3<String, String, Long>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void apply(Tuple2<String, String> key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            Iterator<Long> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                Long value = iterator.next();
                collector.collect(Tuple3.of(key.f0, key.f1, value));
            }
        }
    }

    public static class CountAggDoctorGroupByOrgCode implements AggregateFunction<DoctorBasicInfo, Tuple2<BloomFilter<String>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.unencodedCharsFunnel(),1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(DoctorBasicInfo doctorBasicInfo, Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2) {
            if (!bloomFilterLongTuple2.f0.mightContain(doctorBasicInfo.getOrgCode() + "_" +
                    doctorBasicInfo.getInDocCode())) {
                bloomFilterLongTuple2.f0.put(doctorBasicInfo.getOrgCode() + "_" +
                        doctorBasicInfo.getInDocCode());
                bloomFilterLongTuple2.f1 += 1;
            }
            return bloomFilterLongTuple2;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2) {
            return bloomFilterLongTuple2.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<String>, Long> acc1) {
            return null;
        }
    }

    public static class windowFunctionDoctorAll implements WindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
            Iterator<Long> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                Long value = iterator.next();
                collector.collect(Tuple2.of(key, value));
            }
        }
    }

    public static class CountAggDoctor implements AggregateFunction<DoctorBasicInfo, Tuple2<BloomFilter<String>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.unencodedCharsFunnel(), 1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(DoctorBasicInfo doctorBasicInfo, Tuple2<BloomFilter<String>, Long> bloomFilterAccumulator) {
            if (!bloomFilterAccumulator.f0.mightContain(doctorBasicInfo.getOrgCode() + "_" +
                    doctorBasicInfo.getInDocCode())){
                bloomFilterAccumulator.f0.put(doctorBasicInfo.getOrgCode() + "_" +
                        doctorBasicInfo.getInDocCode());
                bloomFilterAccumulator.f1 += 1;
            }
            return bloomFilterAccumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> bloomFilterAccumulator) {
            return bloomFilterAccumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<String>, Long> acc1) {
            return null;
        }
    }
}

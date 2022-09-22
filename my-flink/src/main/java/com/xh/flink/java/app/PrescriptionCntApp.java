package com.xh.flink.java.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xh.flink.java.common.ApplicationPropertiesConfig;
import com.xh.flink.java.pojo.Prescription;
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
 * @Classname PrescriptionCntApp
 * @Description 处方统计
 * @Version 1.0.0
 * @Date 2022/9/21 16:34
 * @Created by xiaohuan
 */
@Slf4j
public class PrescriptionCntApp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ApplicationPropertiesConfig propertiesConfig = new ApplicationPropertiesConfig();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(propertiesConfig.getProperty("db.ip"))
                .port(Integer.parseInt(propertiesConfig.getProperty("db.port")))
                .databaseList(propertiesConfig.getProperty("db.databaseName"))
                .tableList(propertiesConfig.getProperty("db.databaseName") + "." + propertiesConfig.getProperty("db.prescription"))
                .username(propertiesConfig.getProperty("db.username"))
                .password(propertiesConfig.getProperty("db.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将SourceRecord 转成 JSON 字符串，格式如下注释：
                .build();

        DataStreamSource<String> sourceData = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "prescription");

        DataStream<Prescription> filterData =
                sourceData.filter(new FilterFunction<String>() {
                            @Override
                            public boolean filter(String line) throws Exception {
                                JSONObject record = JSON.parseObject(line);
                                return record.get("before") == null;
                            }
                        }).uid("FilterInsert")
                        .map(line -> {
                            JSONObject record = JSON.parseObject(line);
                            Prescription prescription = null;
                            if (record.get("before") == null){
                                try {
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
                                    prescription = JSON.parseObject(record.get("after").toString(), Prescription.class);
                                    prescription.setBirthday(prescription.getBirthday()!=null? dateFormat1.format(new Date(Long.valueOf(prescription.getBirthday()) - 28800000)) : null);
                                    prescription.setPresTime(prescription.getPresTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getPresTime()) - 28800000)) : null);
                                    prescription.setReviewTime(prescription.getReviewTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getReviewTime()) - 28800000)) : null);
                                    prescription.setTrialTime(prescription.getTrialTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getTrialTime()) - 28800000)) : null);
                                    prescription.setUpdatedLast(prescription.getUpdatedLast()!=null?new Timestamp(prescription.getUpdatedLast().getTime() - 28800000) : null);
                                }catch (Exception e){
                                    log.error("解析错误：" + line);
                                    e.printStackTrace();
                                }
                            }
                            return prescription;
                        }).uid("MapConvertsJSONToObject")
                        /*.filter(new FilterFunction<Prescription>() {
                            @Override
                            public boolean filter(Prescription prescription) throws Exception {
                                String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                                return prescription.getPresTime().substring(0, 10).equals(currentDate);
                            }
                        }).uid("FilterCurrentDateData")*/;

        DataStream<Tuple2<String, Long>> countAllPresResult =
                filterData.keyBy(new KeySelector<Prescription, String>() {
                    @Override
                    public String getKey(Prescription prescription) throws Exception {
                        return prescription.getPresTime().substring(0, 10);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .aggregate(new CountAggPrescription(), new windowFunctionAll());

        DataStream<Tuple3<String, String, Long>> countGroupByOrgCodeResult =
                filterData.keyBy(new KeySelector<Prescription, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Prescription prescription) throws Exception {
                        return Tuple2.of(prescription.getPresTime().substring(0, 10), prescription.getOrgCode());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .aggregate(new CountAggGroupByOrgCode(), new windowFunctionGroupByOrgCode());

        SinkFunction<Tuple2<String, Long>> sinkAllPresMysql = new MySQLJDBCSink<>(
                "INSERT INTO supervise_stat_cnt_all_df\n" +
                        "(business_date, business_type, target_type, target_name, target_value)\n" +
                        "VALUES(?, ?, ?, ?, ?)\n" +
                        "ON DUPLICATE KEY UPDATE target_value=?"
                ,
                new JdbcStatementBuilder<Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple2<String, Long> result) throws SQLException {
                        ps.setString(1, result.f0);
                        ps.setString(2, "prescription");
                        ps.setString(3, "count");
                        ps.setString(4, "prescription_cnt_1d");
                        ps.setLong(5, result.f1);
                        ps.setLong(6, result.f1);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countAllPresResult.addSink(sinkAllPresMysql);

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
                        ps.setString(2, "prescription");
                        ps.setString(3, result.f1);
                        ps.setString(4, "count");
                        ps.setString(5, "prescription_org_cnt_1d");
                        ps.setLong(6, result.f2);
                        ps.setLong(7, result.f2);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countGroupByOrgCodeResult.addSink(sinkGroupByOrgCodeMysql);

        env.execute("prescription" + System.currentTimeMillis());
    }

    public static class windowFunctionGroupByOrgCode implements WindowFunction<Long, Tuple3<String, String, Long>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void apply(Tuple2<String, String> key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            Iterator<Long> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                Long value = iterator.next();
                collector.collect(Tuple3.of(key.f0, key.f1, value));
            }
        }
    }

    public static class CountAggGroupByOrgCode implements AggregateFunction<Prescription, Tuple2<BloomFilter<String>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.unencodedCharsFunnel(),1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(Prescription prescription, Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2) {
            if (!bloomFilterLongTuple2.f0.mightContain(prescription.getOrgCode() + "_" + prescription.getId())) {
                bloomFilterLongTuple2.f0.put(prescription.getOrgCode() + "_" + prescription.getId());
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

    public static class windowFunctionAll implements WindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
            Iterator<Long> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                Long value = iterator.next();
                collector.collect(Tuple2.of(key, value));
            }
        }
    }

    public static class CountAggPrescription implements AggregateFunction<Prescription, Tuple2<BloomFilter<Long>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Prescription prescription, Tuple2<BloomFilter<Long>, Long> bloomFilterAccumulator) {
            if (!bloomFilterAccumulator.f0.mightContain(prescription.getId())){
                bloomFilterAccumulator.f0.put(prescription.getId());
                bloomFilterAccumulator.f1 += 1;
            }
            return bloomFilterAccumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> bloomFilterAccumulator) {
            return bloomFilterAccumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<Long>, Long> acc1) {
            return null;
        }
    }
}

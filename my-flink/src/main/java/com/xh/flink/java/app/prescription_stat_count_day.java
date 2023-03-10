package com.xh.flink.java.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xh.flink.java.common.DateUtils;
import com.xh.flink.java.common.PropertiesConfig;
import com.xh.flink.java.pojo.Prescription;
import com.xh.flink.java.sink.MySQLJDBCSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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

/**
 * @Classname prescription_stat_count_day
 * @Description TODO
 * @Version 1.0.0
 * @Date 2022/11/7 10:42
 * @Created by xiaohuan
 */
@Slf4j
public class prescription_stat_count_day {

    public static void main(String[] args) throws Exception {

        String endDate = DateUtils.getCurrentDate();

        String beginDate = DateUtils.getCurrentDate(-15);

        String[] tables = null;

        PropertiesConfig propertiesConfig = new PropertiesConfig();

        if (DateUtils.getFormatDate(beginDate, "yyyyMM").equals(DateUtils.getFormatDate(endDate, "yyyyMM"))){
            tables = new String[1];
            tables[0] = propertiesConfig.getProperty("db.databaseName") + "." +
                    propertiesConfig.getProperty("db.prescription") + "_" + DateUtils.getFormatDate(endDate, "yyyyMM");
        } else {
            tables = new String[2];
            tables[0] = propertiesConfig.getProperty("db.databaseName") + "." +
                    propertiesConfig.getProperty("db.prescription") + "_" + DateUtils.getFormatDate(endDate, "yyyyMM");
            tables[1] = propertiesConfig.getProperty("db.databaseName") + "." +
                    propertiesConfig.getProperty("db.prescription") + "_" + DateUtils.getFormatDate(beginDate, "yyyyMM");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(propertiesConfig.getProperty("db.ip"))
                .port(Integer.parseInt(propertiesConfig.getProperty("db.port")))
                .databaseList(propertiesConfig.getProperty("db.databaseName"))
                .tableList(propertiesConfig.getProperty("db.databaseName") + "." + "prescription_202.*")
//                .tableList(propertiesConfig.getProperty("db.databaseName") + "." +
//                        propertiesConfig.getProperty("db.prescription") + "_" )
//                .tableList(tables)
//                .scanNewlyAddedTableEnabled(true)
//                .includeSchemaChanges(true)
                .username(propertiesConfig.getProperty("db.username"))
                .password(propertiesConfig.getProperty("db.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将SourceRecord 转成 JSON 字符串，格式如下注释
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> sourceData = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "prescription");

        DataStream<Prescription> filterData =
                sourceData.filter(new FilterFunction<String>() {
                            @Override
                            public boolean filter(String line) throws Exception {
                                JSONObject record = JSON.parseObject(line);
                                JSONObject source = JSON.parseObject(record.get("source").toString());
                                String table = source.get("table").toString();
                                String currentTable = "prescription_" + DateUtils.getFormatDate(DateUtils.getCurrentDate(), "yyyyMM");
                                return record.get("before") == null && currentTable.equals(table);
                            }
                        }).name("FilterInsert").uid("FilterInsert")
                        .map(line -> {
                            JSONObject record = JSON.parseObject(line);
                            Prescription prescription = null;
                            if (record.get("before") == null){
                                try {
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
                                    prescription = JSON.parseObject(record.get("after").toString(), Prescription.class);
                                    prescription.setOrgCode(prescription.getOrgCode()!=null? prescription.getOrgCode() : "");
                                    prescription.setBirthday(prescription.getBirthday()!=null? dateFormat1.format(new Date(Long.valueOf(prescription.getBirthday()) - 28800000)) : null);
                                    prescription.setPresTime(prescription.getPresTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getPresTime()) - 28800000)) : "");
                                    prescription.setReviewTime(prescription.getReviewTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getReviewTime()) - 28800000)) : null);
                                    prescription.setTrialTime(prescription.getTrialTime()!=null?dateFormat.format(new Date(Long.valueOf(prescription.getTrialTime()) - 28800000)) : null);
                                    prescription.setUpdatedLast(prescription.getUpdatedLast()!=null?new Timestamp(prescription.getUpdatedLast().getTime() - 28800000) : null);
                                }catch (Exception e){
                                    log.error("解析错误：" + line);
                                    e.printStackTrace();
                                }
                            }
                            return prescription;
                        }).name("MapConvertsJSONToObject").uid("MapConvertsJSONToObject")
                        .filter(new FilterFunction<Prescription>() {
                            @Override
                            public boolean filter(Prescription prescription) throws Exception {
                                String currentDate = DateUtils.getCurrentDate();
                                return DateUtils.dayBetween(DateUtils.getFormatDate(prescription.getPresTime(), "yyyy-MM-dd"), currentDate) == 0;
                            }
                        }).uid("FilterCurrentDateData");

        DataStream<Tuple2<String, Long>> countAllPresResult =
                filterData.keyBy(new KeySelector<Prescription, String>() {
                            @Override
                            public String getKey(Prescription prescription) throws Exception {
                                return prescription.getPresTime()!=null&&prescription.getPresTime().length()>10?DateUtils.getFormatDate(prescription.getPresTime(), "yyyyMMdd"):"";
                            }
                        })
                        .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                        .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                        .aggregate(new BloomFilterCntAgg(), new WindowResult()).name("CountAggPrescription").uid("CountAggPrescription").startNewChain();

        SinkFunction<Tuple2<String, Long>> sinkAllPresMysql = new MySQLJDBCSink<>(
                "INSERT INTO stat_count_day\n" +
                        "(today_pre_count, `date`)\n" +
                        "VALUES(?, ?)\n" +
                        "ON DUPLICATE KEY UPDATE today_pre_count=?"
                ,
                new JdbcStatementBuilder<Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple2<String, Long> result) throws SQLException {
                        ps.setLong(1, result.f1);
                        ps.setString(2, result.f0);
                        ps.setLong(3, result.f1);
                    }
                },
                propertiesConfig.getProperty("db.w.databaseName"), 200, 1000, 5).getSink();

        countAllPresResult.addSink(sinkAllPresMysql).name("sinkAllPresMysql").uid("sinkAllPresMysql");

        env.execute("prescription_stat_count_day");
    }

    /**
     * @description 布隆过滤器去重统计
     * @param
     * @return
     * @author xiaohuan
     * @date 2022/12/6 16:27:03
     */
    public static class BloomFilterCntAgg implements AggregateFunction<Prescription, Tuple2<BloomFilter<Long>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 10000000, 0.01), 0L);
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

    /**
     * @description 不去重统计
     * @param
     * @return
     * @author xiaohuan
     * @date 2022/12/6 16:29:12
     */
    public static class cntIdAgg implements AggregateFunction<Prescription,  Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Prescription value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>.Context ctx, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
            collector.collect(Tuple2.of(key, iterable.iterator().next()));
        }
    }
}

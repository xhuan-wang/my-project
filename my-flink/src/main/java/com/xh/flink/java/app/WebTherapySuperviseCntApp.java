package com.xh.flink.java.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xh.flink.java.common.ApplicationPropertiesConfig;
import com.xh.flink.java.pojo.WebTherapySuperviseInfo;
import com.xh.flink.java.sink.MySQLJDBCSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
 * @Classname WebTherapySuperviseCntApp
 * @Description 在线诊疗统计：web_therapy_supervise_info
 * @Version 1.0.0
 * @Date 2022/9/20 10:09
 * @Created by xiaohuan
 */
@Slf4j
public class WebTherapySuperviseCntApp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ApplicationPropertiesConfig propertiesConfig = new ApplicationPropertiesConfig();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(propertiesConfig.getProperty("db.ip"))
                .port(Integer.parseInt(propertiesConfig.getProperty("db.port")))
                .databaseList(propertiesConfig.getProperty("db.databaseName"))
                .tableList(propertiesConfig.getProperty("db.databaseName") + "." + propertiesConfig.getProperty("db.tableName"))
                .username(propertiesConfig.getProperty("db.username"))
                .password(propertiesConfig.getProperty("db.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将SourceRecord 转成 JSON 字符串，格式如下注释：
                .build();

        /**
         * @description flinkcdc读取binlog日志格式：
         * 1、插入（before=null）：
         * {"before":null,"after":{"id":761474912580861952,"org_code":"WEBH058","org_name":"镁信（海南）互联网医院","visit_time":1659524400000,"visit_finish_time":"-1","visit_dept_code":"07","visit_dept_name":"儿科","visit_doc_code":"10852","visit_doc_name":"验收数据一医生","pt_id":"d8c463aa319e4a67851e6ebf12efafec","med_rd_no":"R202007031052273281235","revisit_flag":0,"first_visit_org_code":"镁信（海南）互联网医院","first_visit_org_name":"WEBH058","med_class_code":"6","med_class_name":"预约","price":"ZA==","pt_no":"李加加","ge_code":"1","ge_name":"男","pt_age":29,"pt_birthdate_wait":652838400000,"valid_cert_code":null,"valid_cert_name":null,"id_no":"230229199009095431","pt_tel":"17778787876","pt_district":"460100","consult_doc_list":"[{\"consult_doc_code\":\"29891d5099b411eab56000163e061a23\",\"consult_doc_name\":\"彭医生\"}]","diseases_code":"Y98","diseases_name":"与生活方式有关的情况","complaint_content":"鼻塞、打喷嚏","present_illness":"-1","past_history":"-1","ask_or_med":"0","handle_status":2,"updated_last":"2022-08-03T14:26:07Z","pt_birthdate":"1990-09-09"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1663656403000,"snapshot":"false","db":"spvr_dev","sequence":null,"table":"web_therapy_supervise_info_202208","server_id":1,"gtid":null,"file":"mysql-bin.000183","pos":3319691,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1663656403909,"transaction":null}
         * 2、更新（before!=null and after!=null）：
         * {"before":{"id":758976375402020816,"org_code":"WEBH009","org_name":"施强药业","visit_time":1659524404000,"visit_finish_time":"2021-01-03 12:40:19","visit_dept_code":"-1","visit_dept_name":"中医疑难病科","visit_doc_code":"6e7cbf7ab423f901ddf7404a8ceb1811","visit_doc_name":"毛峪泉","pt_id":"ccd5606d7ff8d6901e7270b521ce96ff","med_rd_no":"20210103105952423816","revisit_flag":1,"first_visit_org_code":"8b4b6f2a0cd74f4fd30a826687113350","first_visit_org_name":"青医附院","med_class_code":"1","med_class_name":"图文问诊","price":"ZA==","pt_no":"x","ge_code":"2","ge_name":"女","pt_age":23,"pt_birthdate_wait":null,"valid_cert_code":null,"valid_cert_name":null,"id_no":"-1","pt_tel":"15665705119","pt_district":"210000","consult_doc_list":"[]","diseases_code":"-1","diseases_name":"暂无","complaint_content":"开药","present_illness":"暂无","past_history":"无","ask_or_med":"1","handle_status":2,"updated_last":"2021-06-11T00:02:59Z","pt_birthdate":"-1"},"after":{"id":758976375402020816,"org_code":"WEBH009","org_name":"施强药业","visit_time":1659524404000,"visit_finish_time":"2021-01-03 12:40:19","visit_dept_code":"07","visit_dept_name":"中医疑难病科","visit_doc_code":"6e7cbf7ab423f901ddf7404a8ceb1811","visit_doc_name":"毛峪泉","pt_id":"ccd5606d7ff8d6901e7270b521ce96ff","med_rd_no":"20210103105952423816","revisit_flag":1,"first_visit_org_code":"8b4b6f2a0cd74f4fd30a826687113350","first_visit_org_name":"青医附院","med_class_code":"1","med_class_name":"图文问诊","price":"ZA==","pt_no":"x","ge_code":"2","ge_name":"女","pt_age":23,"pt_birthdate_wait":null,"valid_cert_code":null,"valid_cert_name":null,"id_no":"-1","pt_tel":"15665705119","pt_district":"210000","consult_doc_list":"[]","diseases_code":"-1","diseases_name":"暂无","complaint_content":"开药","present_illness":"暂无","past_history":"无","ask_or_med":"1","handle_status":2,"updated_last":"2021-06-11T00:02:59Z","pt_birthdate":"-1"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1663643554000,"snapshot":"false","db":"spvr_dev","sequence":null,"table":"web_therapy_supervise_info_202208","server_id":1,"gtid":null,"file":"mysql-bin.000182","pos":5750777,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1663643554745,"transaction":null}
         * 3、删除（after=null）：
         * {"before":{"id":761474912580861952,"org_code":"WEBH058","org_name":"镁信（海南）互联网医院","visit_time":1659524400000,"visit_finish_time":"-1","visit_dept_code":"07","visit_dept_name":"儿科","visit_doc_code":"10852","visit_doc_name":"验收数据一医生","pt_id":"d8c463aa319e4a67851e6ebf12efafec","med_rd_no":"R202007031052273281235","revisit_flag":0,"first_visit_org_code":"镁信（海南）互联网医院","first_visit_org_name":"WEBH058","med_class_code":"6","med_class_name":"预约","price":"ZA==","pt_no":"李加加","ge_code":"1","ge_name":"男","pt_age":29,"pt_birthdate_wait":652838400000,"valid_cert_code":null,"valid_cert_name":null,"id_no":"230229199009095431","pt_tel":"17778787876","pt_district":"460100","consult_doc_list":"[{\"consult_doc_code\":\"29891d5099b411eab56000163e061a23\",\"consult_doc_name\":\"彭医生\"}]","diseases_code":"Y98","diseases_name":"与生活方式有关的情况","complaint_content":"鼻塞、打喷嚏","present_illness":"-1","past_history":"-1","ask_or_med":"0","handle_status":2,"updated_last":"2022-08-03T14:26:07Z","pt_birthdate":"1990-09-09"},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1663656337000,"snapshot":"false","db":"spvr_dev","sequence":null,"table":"web_therapy_supervise_info_202208","server_id":1,"gtid":null,"file":"mysql-bin.000183","pos":3225489,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1663656337957,"transaction":null}
         * 其中：
         * c = create-插入
         * u = update-更新
         * d = delete-删除
         * r = read (applies to only snapshots-仅适用于快照)
         * @author xiaohuan
         * @date 2022/9/20 14:54:45
         */
        DataStreamSource<String> sourceData = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "web_therapy_supervise_info");

        DataStream<WebTherapySuperviseInfo> filterData =
                sourceData.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        JSONObject record = JSON.parseObject(line);
                        return record.get("before") == null;
                    }
                }).uid("FilterInsert")
                .map(line -> {
                    JSONObject record = JSON.parseObject(line);
                    WebTherapySuperviseInfo therapySuperviseInfo = null;
                    if (record.get("before") == null){
                        try {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
                            therapySuperviseInfo = JSON.parseObject(record.get("after").toString(), WebTherapySuperviseInfo.class);
                            therapySuperviseInfo.setVisitTime(therapySuperviseInfo.getVisitTime()!=null? dateFormat.format(new Date(Long.valueOf(therapySuperviseInfo.getVisitTime()) - 28800000)) : null);
                            therapySuperviseInfo.setPtBirthdateWait(therapySuperviseInfo.getPtBirthdateWait()!=null?dateFormat1.format(new Date(Long.valueOf(therapySuperviseInfo.getPtBirthdateWait()) - 28800000)) : null);
                            therapySuperviseInfo.setUpdatedLast(therapySuperviseInfo.getUpdatedLast()!=null?new Timestamp(therapySuperviseInfo.getUpdatedLast().getTime() - 28800000) : null);
                        }catch (Exception e){
                            log.error("解析错误：" + line);
                            e.printStackTrace();
                        }
                    }
//            System.out.println("before: " + record.get("before"));
//            System.out.println("after: " + record.get("after"));
//            System.out.println("op: " + record.get("op"));
                    return therapySuperviseInfo;
                }).uid("MapConvertsJSONToObject")
                /*.filter(new FilterFunction<WebTherapySuperviseInfo>() {
                    @Override
                    public boolean filter(WebTherapySuperviseInfo webTherapySuperviseInfo) throws Exception {
                        String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                        return webTherapySuperviseInfo.getVisitTime().substring(0, 10).equals(currentDate);
                    }
                }).uid("FilterCurrentDateData")*/
        /*.print().setParallelism(1)*/;

        DataStream<Tuple2<String, Long>> countAllResult =
                filterData.keyBy(new KeySelector<WebTherapySuperviseInfo, String>() {
                        @Override
                        public String getKey(WebTherapySuperviseInfo webTherapySuperviseInfo) throws Exception {
                            return webTherapySuperviseInfo.getVisitTime().substring(0, 10);
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                    .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                    .aggregate(new CountAgg(), new WindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>() {
                        @Override
                        public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                            Iterator<Long> iterator = iterable.iterator();
                            while (iterator.hasNext()) {
                                Long value = iterator.next();
                                collector.collect(Tuple2.of(key, value));
                            }
                        }
                    });

        DataStream<Tuple3<String, String, Long>> countGroupByOrgCodeResult =
                filterData.keyBy(new KeySelector<WebTherapySuperviseInfo, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(WebTherapySuperviseInfo webTherapySuperviseInfo) throws Exception {
                        return Tuple2.of(webTherapySuperviseInfo.getVisitTime().substring(0, 10), webTherapySuperviseInfo.getOrgCode());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .aggregate(new CountAggGroupByOrgCode(), new WindowFunction<Long, Tuple3<String, String, Long>, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple2<String, String> key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        Iterator<Long> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            Long value = iterator.next();
                            collector.collect(Tuple3.of(key.f0, key.f1, value));
                        }
                    }
                });

        SinkFunction<Tuple2<String, Long>> sinkAllMysql = new MySQLJDBCSink<>(
                "INSERT INTO supervise_stat_cnt_all_df\n" +
                        "(business_date, business_type, target_type, target_name, target_value)\n" +
                        "VALUES(?, ?, ?, ?, ?)\n" +
                        "ON DUPLICATE KEY UPDATE target_value=?"
                ,
                new JdbcStatementBuilder<Tuple2<String, Long>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple2<String, Long> result) throws SQLException {
                        /*Field[] fields = hsjcResult.getClass().getDeclaredFields();
                        try {
                            SinkSingleClickHouse.setPs(ps, fields, hsjcResult);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }*/
                        ps.setString(1, result.f0);
                        ps.setString(2, "therapy");
                        ps.setString(3, "count");
                        ps.setString(4, "therapy_cnt_1d");
                        ps.setLong(5, result.f1);
                        ps.setLong(6, result.f1);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countAllResult.addSink(sinkAllMysql);

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
                        ps.setString(2, "therapy");
                        ps.setString(3, result.f1);
                        ps.setString(4, "count");
                        ps.setString(5, "therapy_org_cnt_1d");
                        ps.setLong(6, result.f2);
                        ps.setLong(7, result.f2);
                    }
                },
                propertiesConfig.getProperty("db.databaseName"), 200, 1000, 5).getSink();

        countGroupByOrgCodeResult.addSink(sinkGroupByOrgCodeMysql);

        env.execute("WebTherapySuperviseCntApp" + System.currentTimeMillis());
    }

    public static class CountAggGroupByOrgCode implements AggregateFunction<WebTherapySuperviseInfo, Tuple2<BloomFilter<String>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.unencodedCharsFunnel(),1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(WebTherapySuperviseInfo webTherapySuperviseInfo, Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2) {
            if (!bloomFilterLongTuple2.f0.mightContain(webTherapySuperviseInfo.getOrgCode() + "_" + webTherapySuperviseInfo.getId())) {
                bloomFilterLongTuple2.f0.put(webTherapySuperviseInfo.getOrgCode() + "_" + webTherapySuperviseInfo.getId());
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

    public static class CountAgg implements AggregateFunction<WebTherapySuperviseInfo, Tuple2<BloomFilter<Long>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(WebTherapySuperviseInfo webTherapySuperviseInfo, Tuple2<BloomFilter<Long>, Long> bloomFilterAccumulator) {
            if (!bloomFilterAccumulator.f0.mightContain(webTherapySuperviseInfo.getId())){
                bloomFilterAccumulator.f0.put(webTherapySuperviseInfo.getId());
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

    public static class countAggKeyProcessFunc extends KeyedProcessFunction<Tuple2<String, String>, WebTherapySuperviseInfo, Tuple3<String, String, Long>> {

        //保存分组数据去重后id的布隆过滤器，加transient禁止参与反序列化
        private transient ValueState<BloomFilter> bloomFilterValueState = null;
        //保存去重后总id数的state
        private transient ValueState<Long> uidCountState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<BloomFilter> bloomDescriptor = new ValueStateDescriptor<BloomFilter>(
                    "actId-typ",
                    // 数据类型的class对象，因为是布隆过滤器，所有要使用这种方式来拿
                    TypeInformation.of(new TypeHint<BloomFilter>() {
                    })
            );
            ValueStateDescriptor<Long> uidCountDescriptor = new ValueStateDescriptor<Long>(
                    "uid-count",
                    Long.class
            );

            bloomFilterValueState = getRuntimeContext().getState(bloomDescriptor);
            uidCountState = getRuntimeContext().getState(uidCountDescriptor);
        }

        @Override
        public void processElement(WebTherapySuperviseInfo webTherapySuperviseInfo, KeyedProcessFunction<Tuple2<String, String>, WebTherapySuperviseInfo, Tuple3<String, String, Long>>.Context context, Collector<Tuple3<String, String, Long>> collector) throws Exception {

            BloomFilter bloomFilter = bloomFilterValueState.value();
            Long uidCount = uidCountState.value();

            if (bloomFilter == null) {
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
                uidCount = 0L;
            }

            //布隆过滤器中是否不包含uid,是的话就返回false
            if (!bloomFilter.mightContain(webTherapySuperviseInfo.getId())) {
                bloomFilter.put(webTherapySuperviseInfo.getId()); //不包含就添加进去
                uidCount += 1;
            }

            bloomFilterValueState.update(bloomFilter);
            uidCountState.update(uidCount);

            collector.collect(Tuple3.of(webTherapySuperviseInfo.getVisitTime().substring(0, 10), webTherapySuperviseInfo.getOrgCode(), uidCount));
        }
    }

    public static class WindowResult implements WindowFunction<Long, WebTherapySuperviseInfo, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<WebTherapySuperviseInfo> collector) throws Exception {

        }
    }
}

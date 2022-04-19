package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import com.xh.flink.java.source.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * @author: xhuan_wang
 * @Title: SinkCustomToHBase
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 17:12
 */
public class SinkCustomToHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .addSink(new RichSinkFunction<Event>() {
                    public org.apache.hadoop.conf.Configuration configuration;
                    public Connection connection;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        configuration = HBaseConfiguration.create();
                        configuration.set("hbase.zookeeper.quorum", "");
                        connection = ConnectionFactory.createConnection(configuration);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        connection.close();
                    }

                    @Override
                    public void invoke(Event value, Context context) throws Exception {
                        super.invoke(value, context);
                        Table table = connection.getTable(TableName.valueOf("test"));
                        Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                                value.toString().getBytes(StandardCharsets.UTF_8),
                                "1".getBytes(StandardCharsets.UTF_8));
                        table.put(put);
                        table.close();
                    }
                });

        env.execute();
    }
}

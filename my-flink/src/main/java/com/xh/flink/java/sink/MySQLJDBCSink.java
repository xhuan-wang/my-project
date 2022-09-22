package com.xh.flink.java.sink;

import com.xh.flink.java.common.ApplicationPropertiesConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author: xhuan_wang
 * @Title: SinkSingleClickHouse
 * @ProjectName: flink-to-others
 * @Description:
 * @date: 2022-05-20 15:27
 */
public class MySQLJDBCSink<T> {

    private final static String NA = "null";
    private final SinkFunction<T> sink;

    /**
     * 获取MySQLJDBCSink sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     * @param database             表所在的数据库
     */
    public MySQLJDBCSink(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder, String database) {
        ApplicationPropertiesConfig propertiesConfig = new ApplicationPropertiesConfig();
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(propertiesConfig.getProperty("db.url") + database)
                        .withDriverName(propertiesConfig.getProperty("db.driver"))
                        .withUsername(propertiesConfig.getProperty("db.username"))
                        .withPassword(propertiesConfig.getProperty("db.password"))
                        .build()
        );
    }

    /**
     * 获取MySQLJDBCSink sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     * @param database             表所在的数据库
     * @param batchIntervalMs      提交条件之：间隔
     * @param batchSize            提交条件之：数据量
     * @param maxRetries           提交重试次数
     */
    public MySQLJDBCSink(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder, String database,
                         int batchIntervalMs, int batchSize, int maxRetries) {
        ApplicationPropertiesConfig propertiesConfig = new ApplicationPropertiesConfig();
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(batchIntervalMs)
                        .withBatchSize(batchSize)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(propertiesConfig.getProperty("db.url") + database)
                        .withDriverName(propertiesConfig.getProperty("db.driver"))
                        .withUsername(propertiesConfig.getProperty("db.username"))
                        .withPassword(propertiesConfig.getProperty("db.password"))
                        .build()
        );
    }

    public SinkFunction<T> getSink() {
        return sink;
    }

    /**
     * 用于设置MySQLJDBCSink PreparedStatement的通用方法
     *
     * @param ps     PreparedStatement实例
     * @param fields 通过”实例对象.getClass().getDeclaredFields()“获得
     * @param bean   实例对象
     * @throws IllegalAccessException field.get抛出的错误
     * @throws SQLException           ps.set抛出的错误
     */
    public static void setPs(PreparedStatement ps, Field[] fields, Object bean) throws IllegalAccessException, SQLException {
        for (int i = 1; i <= fields.length; i++) {
            Field field = fields[i - 1];
            field.setAccessible(true);
            Object o = field.get(bean);
            if (o == null) {
                ps.setNull(i, 0);
                continue;
            }
            String fieldValue = o.toString();
            if (!NA.equals(fieldValue) && !"".equals(fieldValue)) {
                ps.setObject(i, fieldValue);
            } else {
                ps.setNull(i, 0);
            }
        }
    }
}
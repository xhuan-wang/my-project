package com.xh.flink.java.sink;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: xhuan_wang
 * @Title: MyRedisMapper
 * @ProjectName: my-project
 * @Description:
 * @date: 2022-04-19 14:28
 */
public class MyRedisMapper implements RedisMapper<Event> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "clicks");
    }

    @Override
    public String getKeyFromData(Event event) {
        return event.user;
    }

    @Override
    public String getValueFromData(Event event) {
        return event.url;
    }
}

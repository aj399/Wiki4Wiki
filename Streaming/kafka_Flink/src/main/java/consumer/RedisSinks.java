package consumer;

import java.util.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public RedisSinks{

	public static class TrendingRedisMapper implements RedisMapper<Tuple3<String, Long, Long>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH, "TRENDING");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Long, Long> data) {
            return (String) "trending";
        }

        @Override
        public String getValueFromData(Tuple3<String, Long, Long> data) {
            return data.f0+"\t"+data.f1+"\t"+data.f2;
        }
    }

}
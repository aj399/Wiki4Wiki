//All the redis sinks used in MainStream.java

package consumer;

import java.util.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class RedisSinks{
	
	//Redis mapper to send the trending articles to redis list keyed by 'trending'
	public static class TrendingRedisMapper implements RedisMapper<Tuple8<String, String, Long, Long, Long, Long, Long, Long>>{

		@Override
		public RedisCommandDescription getCommandDescription() {
		    return new RedisCommandDescription(RedisCommand.LPUSH, "TRENDING");
		}

		@Override
		public String getKeyFromData(Tuple8<String, String, Long, Long, Long, Long, Long, Long> data) {
		    return (String) "trending";
		}

		@Override
		public String getValueFromData(Tuple8<String, String, Long, Long, Long, Long, Long, Long> data) {
		    return  data.f0+"\t"+data.f1+"\t"+data.f2+":"+data.f3+":"+data.f4+":"+data.f5+":"+data.f6+":"+data.f7;
		}
	}
	
	//Redis mapper to send the hot articles to redis list keyed by 'Hot Topic'(currently un used)
	public static class HotRedisMapper implements RedisMapper<Tuple3<String, String, Long>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH, "HOT TOPIC");
        }

        @Override
        public String getKeyFromData(Tuple3<String, String, Long> data) {
			Calendar cal = Calendar.getInstance();
			int cMinute = cal.get(Calendar.MINUTE);
            return (String) "Hot Topic"+cMinute;
        }

        @Override
        public String getValueFromData(Tuple3<String, String, Long> data) {
            return data.f0+"\t"+data.f1+"\t"+data.f2;
        }
    }
}

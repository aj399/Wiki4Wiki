package consumer;

import java.util.*;
import consumer.LineSplitters.*;
import consumer.TimeStampers.*;
import consumer.RedisSink.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import java.io.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;




public class MainStream  {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("../streamingConfig.json"));
        JSONObject config =  (JSONObject) obj;
        String WORKERSIP = (String) config.get("WORKERS_IP");
        String MASTERIP  = (String) config.get("MASTER_IP");
        String CONSUMERTOPIC     = (String) config.get("CONSUMER_TOPIC"); 
        String REDISIP = (String) config.get("REDIS_IP"); 
		String PRODUCERTOPIC = (String) config.get("PRODUCER_TOPIC"); 
		String APPNAME = (String) config.get("APP_NAME");
		
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",WORKERSIP);
        properties.setProperty("zookeeper.connect", MASTERIP);
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer09<String> kafkaSource = new FlinkKafkaConsumer09<>(CONSUMERTOPIC, new SimpleStringSchema(), properties);

        DataStream<Tuple3<String, Long, Long>> dataIn = env
            .addSource(kafkaSource)
            .flatMap(new LnSplitArtTimePv())
			.assignTimestampsAndWatermarks(new TimestamperArtTimePv());

        DataStream<String> wikiCount10Min = dataIn
            .keyBy(0)
            .timeWindow(Time.minutes(10), Time.minutes(1))
			.sum(2)
			.map(new MapFunction<Tuple3<String, Long, Long>, String>() {
				@Override
				public String map(Tuple3<String, Long, Long> value) throws Exception {
					return value.f0+"\t"+value.f1+"\t"+value.f2;
				}
			});
		
		FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(PRODUCERTOPIC, new SimpleStringSchema(), properties);   

		
		myProducer.setLogFailuresOnly(false);   
		myProducer.setFlushOnCheckpoint(true);  

		wikiCount10Min.addSink(myProducer);
		
		FlinkKafkaConsumer09<String> kafkaSource10Min = new FlinkKafkaConsumer09<>(PRODUCERTOPIC, new SimpleStringSchema(), properties);

        DataStream<Tuple3<String, String, String>> dataIn10Min = env
            .addSource(kafkaSource10Min)
            .flatMap(new LnSplitArtTimePv1Min())
			.assignTimestampsAndWatermarks(new TimestamperArtTimePv10Min());
		
		
		DataStream<Tuple3<String, String, String>> wikiCounts1Hr = dataIn10Min.keyBy(0)
            .timeWindow(Time.hours(1),Time.minutes(1))
			.reduce(new tuple3Reduce());
		
		DataStream<Tuple7<String, Long, Long, Long, Long, Long, Long>> wikiCounts1HrList = wikiCounts1Hr.flatMap(new LnSplitArtPv1Hr6no());

		DataStream<Tuple3<String, Long, Long>> trending1HrWiki = wikiCounts1HrList.filter(new FilterFunction<Tuple3<String, Long, Long>>() {
			public boolean filter(Tuple3<String, Long, Long> value) { 
				return ((value.f1<=value.f2 ? (value.f2<=value.f3 ? (value.f3<=value.f4 ? (value.f4<=value.f5 ? (value.f5<-value.f6 ? true:false):false):false):false):false)) ;
			}
		});
		
		FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
			.setHost(REDISIP)
			.setPort(6379).build();
		
		trending2Wiki.addSink(new RedisSink<Tuple3<String, Long, Long>>(redisConfig, new TrendingRedisMapper()));
		
        env.execute("SessionizationFn");
    }

	
	public static class tuple3Reduce implements ReduceFunction<Tuple3<String, String, String>> {
		@Override
		public Tuple3<String, String, String> reduce(Tuple3<String, String, String> in1, Tuple3<String, String, String> in2) {
			return new Tuple3<String, String, String>(in1.f0, in1.f1+"\t"+in2.f1, in1.f2+"\t"+in2.f2);
		}
	}
	
	public static class JoinStream implements JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, String, String>> {

		@Override
		public Tuple5<String, String, String, String, String> join( Tuple3<String, String, String> first, Tuple3<String, String, String> second) {
			return new Tuple5<String, String, String, String, String>(first.f0, first.f1, second.f1, first.f2, second.f2);
		}
	}
	

}


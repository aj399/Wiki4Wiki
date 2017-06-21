package consumer;

import java.util.*;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import java.util.concurrent.TimeUnit;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.flink.api.java.utils.ParameterTool;



public class MainStream1  {

    //public static String propertiesFile = "../myjob.properties";

    // Some window parameters
    public static int SPIDERSN  = 8; 
    public static int TUMBLINGW = 60;
    public static int SESSIONWS = 60;

    public static void main(String[] args) throws Exception {

        //ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
        // Set up the flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Reading configureations
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("../myConfig.json"));
        JSONObject jsonObject =  (JSONObject) obj;
        String WORKERSIP = (String) jsonObject.get("WORKERS_IP");
        String MASTERIP  = (String) jsonObject.get("MASTER_IP");
        String TOPIC     = (String) jsonObject.get("TOPIC"); 
        String REDISIP = (String) jsonObject.get("REDIS_IP"); 
		String ProdTopic = "prodTopic";
        // Kafka connector
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",WORKERSIP);
        properties.setProperty("zookeeper.connect", MASTERIP);
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer09<String> kafkaSource = new FlinkKafkaConsumer09<>(TOPIC, new SimpleStringSchema(), properties);

        //Input string to tupleof 4: <ID, Time-in, Time-out, Count>
        DataStream<Tuple3<String, Long, Long>> dataIn = env
            .addSource(kafkaSource)
            .flatMap(new LineSplitter()).assignTimestampsAndWatermarks(new CustomTimestamper());

        // Calculate the number of clicks during the given period of time
		//DataStream<Tuple3<String, Long, Long>> viewCount1 = dataIn
            //.keyBy(0)
            //.timeWindow(Time.seconds(10)).sum(2);
        DataStream<String> viewCount1 = dataIn
            .keyBy(0)
            .timeWindow(Time.seconds(5)).sum(2).map(new MapFunction<Tuple3<String, Long, Long>, String>() {
			@Override
			public String map(Tuple3<String, Long, Long> value) throws Exception {
				return ""+value.f0+";"+value.f1+";"+value.f2;
			}
		});
		
		FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>("topics", new SimpleStringSchema(), properties);   // serialization schema

		
		// the following is necessary for at-least-once delivery guarantee
		myProducer.setLogFailuresOnly(false);   // "false" by default
		myProducer.setFlushOnCheckpoint(true);  // "false" by default

		viewCount1.addSink(myProducer);
		
		
		
		
		FlinkKafkaConsumer09<String> kafkaSource1 = new FlinkKafkaConsumer09<>("topics", new SimpleStringSchema(), properties);

        //Input string to tupleof 4: <ID, Time-in, Time-out, Count>
        DataStream<Tuple3<String, String, String>> dataIn1 = env
            .addSource(kafkaSource1)
            .flatMap(new LineSplitter1()).assignTimestampsAndWatermarks(new CustomTimestamper1());
		
		
		DataStream<Tuple3<String, String, String>> dataIn2 = dataIn1.keyBy(0)
            .timeWindow(Time.seconds(25)).reduce(new tuple3Reduce());
		//dataIn2.print();
		
		DataStream<Tuple6<String, Long, Long, Long, Long, Long>> finalIn = dataIn2.flatMap(new LineSplitter2());

		DataStream<Tuple6<String, Long, Long, Long, Long, Long>> finalFilter = finalIn.filter(new FilterFunction<Tuple6<String, Long, Long, Long, Long, Long>>() {
			public boolean filter(Tuple6<String, Long, Long, Long, Long, Long> value) { return ((value.f1<=value.f2 ? (value.f2<=value.f3 ? (value.f3<=value.f4 ? (value.f4<=value.f5 ? true:false):false):false):false)) ;}
		});
		finalFilter.print();
        // Configure the Redis
        //FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost(REDISIP).setPort(6379).build();

        // Sink data to Redis
        //clickcount.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new ViewerCountMapper()));
        //totalcount.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new TotalCountMapper()));
        //detectspider
            //.select("spider")
            //.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new SpidersIDMapper()));
        //usersession.addSink(new RedisSink<Tuple4<String, Long, Long, Integer>>(redisConf, new EngagementMapper()));


        // Execute the Program 
        env.execute("SessionizationFn");
    }

    // figure out what does "serialVersionUID" means. Many codes include it but dont know why....

	
	public static class tuple3Reduce implements ReduceFunction<Tuple3<String, String, String>> {
		@Override
		public Tuple3<String, String, String> reduce(Tuple3<String, String, String> in1, Tuple3<String, String, String> in2) {
			return new Tuple3<String, String, String>(in1.f0, in1.f1+";"+in2.f1, in1.f2+";"+in2.f2);
		}
	}
	
	public static class JoinStream implements JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, String, String>> {

		@Override
		public Tuple5<String, String, String, String, String> join( Tuple3<String, String, String> first, Tuple3<String, String, String> second) {
			return new Tuple5<String, String, String, String, String>(first.f0, first.f1, second.f1, first.f2, second.f2);
		}
	}
	
	public static class CustomTimestamper implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Long>> {


		@Override
		public long extractTimestamp(Tuple3<String, Long, Long> element, long previousElementTimestamp) {
			return element.f1;
		}

		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}

	}

	public static class CustomTimestamper1 implements AssignerWithPeriodicWatermarks<Tuple3<String, String, String>> {


		@Override
		public long extractTimestamp(Tuple3<String, String, String> element, long previousElementTimestamp) {
			return Long.parseLong(element.f1);
		}

		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}

	}
	
    public static class LineSplitter implements FlatMapFunction<String, Tuple3<String, Long, Long >> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Long, Long>> out) {
            String[] words = line.split(";");
            out.collect(new Tuple3<String, Long, Long >(words[0], Long.parseLong(words[1]), 1L));
        }
    }
	
	public static class LineSplitter2 implements FlatMapFunction<Tuple3<String, String, String >, Tuple6<String, Long, Long, Long, Long, Long>> {
        @Override
        public void flatMap(Tuple3<String, String, String > line, Collector<Tuple6<String, Long, Long, Long, Long, Long>> out) {
            String[] counts = line.f2.split(";");
			if (counts.length<5){
				counts = new String[]{"0","1","0","0","0"};
			}
            out.collect(new Tuple6<String, Long, Long, Long, Long, Long>(line.f0, Long.parseLong(counts[0]),Long.parseLong(counts[1]), Long.parseLong(counts[2]), Long.parseLong(counts[3]), Long.parseLong(counts[4])));
        }
    }
	
	public static class LineSplitter1 implements FlatMapFunction<String, Tuple3<String, String, String >> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, String>> out) {
            String[] words = line.split(";");
            out.collect(new Tuple3<String, String, String >(words[0], words[1], words[2]));
        }
    }
	
	
	private static class NameKeySelector implements KeySelector<Tuple3<String, String, String>, String> {
		@Override
		public String getKey(Tuple3<String, String, String> value) {
			return value.f0;
		}
	}
	
	private static class NameKeySelector2 implements KeySelector<Tuple2<String, String>, String> {
		@Override
		public String getKey(Tuple2<String, String> value) {
			return value.f0;
		}
	}


    /*public static class TotalCountMapper implements RedisMapper<Tuple4<String, Long, Long, Long>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "TOTAL_COUNT");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Long, Integer> data) {
            return (String) "totalcount";
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Long, Integer> data) {
            return data.f3.toString();
        }
    }*/


}


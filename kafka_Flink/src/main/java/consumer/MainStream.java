package consumer;

import java.util.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
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



public class MainStream  {

    public static String propertiesFile = "../myjob.properties";

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
        DataStream<Tuple3<String, String, Long>> dataIn = env
            .addSource(kafkaSource)
            .flatMap(new LineSplitter());

        // Calculate the number of clicks during the given period of time
        DataStream<Tuple3<String, String, Integer>> viewCount = dataIn
            .keyBy(0)
            .timeWindow(Time.seconds(5)).sum(2);

		viewCount.print();
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
        env.execute("Sessionization");
    }

    // figure out what does "serialVersionUID" means. Many codes include it but dont know why....

    public static class LineSplitter implements FlatMapFunction<String, Tuple3<String, String, Long >> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, Long>> out) {
            String[] words = line.split(";");
            out.collect(new Tuple3<String, String, Integer >(words[0], words[1], 1));
        }
    }


    public static class TotalCountMapper implements RedisMapper<Tuple4<String, Long, Long, Long>>{

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
    }


}


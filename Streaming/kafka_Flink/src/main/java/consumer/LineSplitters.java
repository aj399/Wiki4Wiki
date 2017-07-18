//LineSplitter function used in MainStream.java
package consumer;
 
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
 
 public class LineSplitters{
	
	//LineSplitter to extract Article name, timestamp and count(1) from input kafka stream, streaming live page request data created by pykafka_producer.py
	public static class LnSplitArtTimePv implements FlatMapFunction<String, Tuple3<String, Long, Long >> {
		@Override
		public void flatMap(String line, Collector<Tuple3<String, Long, Long>> out) throws NumberFormatException, IllegalArgumentException {
			String[] words = line.split("\t");
			if(isVaildIp(words[1]){
				throw new IllegalArgumentException("Invalid Ip Address");
			}
			out.collect(new Tuple3<String, Long, Long >(words[0], Long.parseLong(words[2]), 1L)); //Not considering ip address(words[1]), as it is not useful for now
		}
    	}
	
	//LineSplitter to extract page requests of 10 minute windows in the 1 hr period currently concatenated into a single string
	public static class LnSplitArtPv1Hr6no implements FlatMapFunction<Tuple3<String, String, String >, Tuple8<String, String, Long, Long, Long, Long, Long, Long>> throws IllegalArgumentException {
		@Override
		public void flatMap(Tuple3<String, String, String > line, Collector<Tuple8<String, String, Long, Long, Long, Long, Long, Long>> out) {
			String[] counts = line.f2.split("\t");
				if (counts.length<6){
					throw new IllegalArgumentException("1 hour window not complete"); //If for any case flatmap function for 1 hr window gets called before the hour is complete or all six window values not recieved from kafka
				}
			out.collect(new Tuple8<String, String, Long, Long, Long, Long, Long, Long>(line.f0, line.f1, Long.parseLong(counts[0]),Long.parseLong(counts[1]), Long.parseLong(counts[2]), Long.parseLong(counts[3]), Long.parseLong(counts[4]), Long.parseLong(counts[5])));
		}
    	}
	
	//LineSplitter to extract Article name, timestamp and count from input kafka stream, containing 10 minute aggregated pagerequest data
	public static class LnSplitArtTimePv1Min implements FlatMapFunction<String, Tuple3<String, String, String >> {
		@Override
		public void flatMap(String line, Collector<Tuple3<String, String, String>> out) {
			String[] words = line.split("\t");
			out.collect(new Tuple3<String, String, String >(words[0], words[1], words[2]));
		}
    	}

 }

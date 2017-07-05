package consumer;

import java.util.*;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.java.tuple.Tuple3;

public class TimeStampers{	
	
	public static class TimestamperArtTimePv implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Long>> {


		@Override
		public long extractTimestamp(Tuple3<String, Long, Long> element, long previousElementTimestamp) {
			return element.f1;
		}

		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}

	}

	public static class TimestamperArtTimePv10Min implements AssignerWithPeriodicWatermarks<Tuple3<String, String, String>> {


		@Override
		public long extractTimestamp(Tuple3<String, String, String> element, long previousElementTimestamp) {
			return Long.parseLong(element.f1);
		}

		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}

	}

}
 package consumer;
 
 import org.apache.flink.api.common.functions.FlatMapFunction;
 
 public class LineSplitters{
	 
	public static class LnSplitArtTimePv implements FlatMapFunction<String, Tuple3<String, Long, Long >> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Long, Long>> out) {
            String[] words = line.split("\t");
            out.collect(new Tuple3<String, Long, Long >(words[0], Long.parseLong(words[1]), 1L));
        }
    }
	
	public static class LnSplitArtPv1Hr6no implements FlatMapFunction<Tuple3<String, String, String >, Tuple7<String, Long, Long, Long, Long, Long, Long>> {
        @Override
        public void flatMap(Tuple3<String, String, String > line, Collector<Tuple7<String, Long, Long, Long, Long, Long, Long>> out) {
            String[] counts = line.f2.split("\t");
			if (counts.length<6){
				counts = new String[]{"0","1","0","0","0", "0"};
			}
            out.collect(new Tuple7<String, Long, Long, Long, Long, Long, Long>(line.f0, Long.parseLong(counts[0]),Long.parseLong(counts[1]), Long.parseLong(counts[2]), Long.parseLong(counts[3]), Long.parseLong(counts[4])));
        }
    }
	
	public static class LineSplitter2 implements FlatMapFunction<Tuple3<String, String, String >, Tuple3<String, Long, Long>> {
        @Override
        public void flatMap(Tuple3<String, String, String > line, Collector<Tuple3<String, Long, Long>> out) {
            String[] counts = line.f2.split("\t");
			if (counts.length<2){
				counts = new String[]{"0","1"};
			}
            out.collect(new Tuple3<String, Long, Long>(line.f0, Long.parseLong(counts[0]),Long.parseLong(counts[1])));
        }
    }
	
	public static class LnSplitArtTimePv1Min implements FlatMapFunction<String, Tuple3<String, String, String >> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, String>> out) {
            String[] words = line.split("\t");
            out.collect(new Tuple3<String, String, String >(words[0], words[1], words[2]));
        }
    }

 }
//Reduce function and isValidIp funtion used in MainStream and LineSplitters

package consumer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class UtilFunctions{

  public static class HrTrendReduce implements ReduceFunction<Tuple3<String, String, String>> {
    @Override
    public Tuple3<String, String, String> reduce(Tuple3<String, String, String> in1, Tuple3<String, String, String> in2) {
      return new Tuple3<String, String, String>(in1.f0, in1.f1, in1.f2+"\t"+in2.f2);
    }
  }
  
  public static boolean isValidIP (String ip) {
    try {
        if ( ip == null || ip.isEmpty() ) {
          return false;
        }

        String[] parts = ip.split( "\\." );
        if ( parts.length != 4 ) {
          return false;
        }

        for ( String s : parts ) {
          int i = Integer.parseInt( s );
          if ( (i < 0) || (i > 255) ) {
            return false;
          }
        }
        if ( ip.endsWith(".") ) {
          return false;
        }

        return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

}

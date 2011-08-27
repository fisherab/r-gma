import org.glite.rgma.Consumer;
import org.glite.rgma.QueryTypeWithInterval;
import org.glite.rgma.RGMAPermanentException;
import org.glite.rgma.RGMATemporaryException;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;
import org.glite.rgma.Tuple;
import org.glite.rgma.TupleSet;

public class ResilientConsumer {
  public static void main(String[] args) {

    String select = "SELECT userId, aString, aReal, anInt FROM "
        + "default.userTable";

    try {

      TimeInterval queryInterval = new TimeInterval(30, TimeUnit.SECONDS);

      Consumer consumer = null;
      while (consumer == null) {
        try {
          consumer = new Consumer(select, QueryTypeWithInterval.C,
        		  queryInterval);
        } catch (RGMATemporaryException e) {
          System.err.println("RGMATemporaryException: " + e.getMessage()
              + " - will retry in 60s");
          sleepSeconds(60);
        }
      }

      while (true) {
        try {
          TupleSet ts = consumer.pop(2000);
          if (ts.getData().size() == 0) {
            sleepSeconds(2);
          } else {
            
            for (Tuple t : ts.getData()) {
              System.out.print("userId=" + t.getString(0) + ", ");
              System.out.print("aString=" + t.getString(1) + ", ");
              System.out.print("aReal=" + t.getFloat(2) + ", ");
              System.out.println("anInt=" + t.getInt(3));
            }
            
          }
          if (!ts.getWarning().equals("")) {
            System.out.println("WARNING: " + ts.getWarning());
          }
        } catch (RGMATemporaryException e) {
          System.err.println("RGMATemporaryException: " + e.getMessage()
              + " - will retry in 60s");
          sleepSeconds(60);
        }
      }

    } catch (RGMAPermanentException e) {
      System.err.println("RGMAPermanentException: " + e.getMessage());
      System.exit(1);
    }

  }

  private static void sleepSeconds(int seconds) {
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException ie) {
    }
  }

}

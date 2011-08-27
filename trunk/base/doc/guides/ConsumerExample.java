import org.glite.rgma.Consumer;
import org.glite.rgma.QueryTypeWithInterval;
import org.glite.rgma.RGMAException;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;
import org.glite.rgma.Tuple;
import org.glite.rgma.TupleSet;

public class ConsumerExample {
  public static void main(String[] args) {

    try {
      TimeInterval historyPeriod = new TimeInterval(10, TimeUnit.MINUTES);
      TimeInterval timeout = new TimeInterval(5, TimeUnit.MINUTES);
      String select = "SELECT userId, aString, aReal, anInt, "
          + "RgmaTimestamp FROM default.userTable";
      Consumer consumer = new Consumer(select, QueryTypeWithInterval.C,
          historyPeriod, timeout);

      TupleSet ts = null;
      do {
        ts = consumer.pop(2000);
        if (ts.getData().size() == 0) {
          try {
            Thread.sleep(2 * 1000);
          } catch (InterruptedException e) {
          }
        } else {

          for (Tuple t : ts.getData()) {
            System.out.print("userId=" + t.getString(0) + ", ");
            System.out.print("aString=" + t.getString(1) + ", ");
            System.out.print("aReal=" + t.getFloat(2) + ", ");
            System.out.println("anInt=" + t.getInt(3));
          }

        }
      } while (!ts.isEndOfResults());

      consumer.close();

    } catch (RGMAException e) {
      System.err.println("ERROR: " + e.toString());
      System.exit(1);
    }

  }
}

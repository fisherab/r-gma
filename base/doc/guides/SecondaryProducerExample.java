import org.glite.rgma.RGMAException;
import org.glite.rgma.RGMAService;
import org.glite.rgma.SecondaryProducer;
import org.glite.rgma.Storage;
import org.glite.rgma.SupportedQueries;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;

public class SecondaryProducerExample {
  public static void main(String[] args) {

    SecondaryProducer secondaryProducer = null;

    try {
      String location = "javaExample";
      Storage storage = Storage.getDatabaseStorage(location);
      secondaryProducer = new SecondaryProducer(storage,
          SupportedQueries.CL);

      String predicate = "";
      TimeInterval historyRetentionPeriod = new TimeInterval(2,
          TimeUnit.HOURS);
      secondaryProducer.declareTable("default.userTable", predicate,
          historyRetentionPeriod);

      TimeInterval terminationInterval = RGMAService
          .getTerminationInterval();

      while (true) {
        secondaryProducer.showSignOfLife();
        try {
          Thread.sleep((terminationInterval
              .getValueAs(TimeUnit.SECONDS) / 3) * 1000);
        } catch (InterruptedException e) {
        }
      }

    } catch (RGMAException e) {
      System.err.println("ERROR: " + e.toString());
      System.exit(1);

    } finally {
      if (secondaryProducer != null) {
        try {
          secondaryProducer.close();
        } catch (RGMAException e) {
        }
      }
    }

  }
}

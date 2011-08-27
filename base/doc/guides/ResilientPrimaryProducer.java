import org.glite.rgma.PrimaryProducer;
import org.glite.rgma.RGMAPermanentException;
import org.glite.rgma.RGMATemporaryException;
import org.glite.rgma.Storage;
import org.glite.rgma.SupportedQueries;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;

public class ResilientPrimaryProducer {
  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: java ResilientPrimaryProducer <userId>");
      System.exit(1);
    }

    String userId = args[0];

    try {

      TimeInterval historyRetentionPeriod = new TimeInterval(50,
          TimeUnit.MINUTES);
      TimeInterval latestRetentionPeriod = new TimeInterval(25,
          TimeUnit.MINUTES);

      PrimaryProducer producer = null;
      
      while (producer == null) {
        try {
          producer = new PrimaryProducer(Storage.getMemoryStorage(),
              SupportedQueries.C);
        } catch (RGMATemporaryException e) {
          System.err.println("RGMATemporaryException: " + e.getMessage()
              + " - will retry in 60s");
          sleepSeconds(60);
        }
      }

      String predicate = "WHERE userId = '" + userId + "'";
      boolean tableDeclared = false;
      
      while (!tableDeclared) {
        try {
          producer.declareTable("default.userTable", predicate,
              historyRetentionPeriod, latestRetentionPeriod);
          tableDeclared = true;
        } catch (RGMATemporaryException e) {
          System.err.println("RGMATemporaryException: " + e.getMessage()
              + " - will retry in 60s");
          sleepSeconds(60);
        }
      }

      int data = 0;
      
      while (true) {
        try {
          String insert = "INSERT INTO default.userTable "
              + "(userId, aString, aReal, anInt) VALUES " + "('" + userId
              + "', 'resilient Java producer', 0.0, " + data + ")";
          producer.insert(insert);
          System.out.println(insert);
          data++;
          sleepSeconds(30);
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

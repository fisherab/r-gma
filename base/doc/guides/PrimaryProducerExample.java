import org.glite.rgma.PrimaryProducer;
import org.glite.rgma.RGMAException;
import org.glite.rgma.Storage;
import org.glite.rgma.SupportedQueries;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;

public class PrimaryProducerExample {
  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: java PrimaryProducerExample <userId>");
      System.exit(1);
    }

    String userId = args[0];

    try {
      PrimaryProducer producer = new PrimaryProducer(Storage
          .getMemoryStorage(), SupportedQueries.C);

      String predicate = "WHERE userId = '" + userId + "'";
      TimeInterval historyRetentionPeriod = new TimeInterval(60,
          TimeUnit.MINUTES);
      TimeInterval latestRetentionPeriod = new TimeInterval(60,
          TimeUnit.MINUTES);
      producer.declareTable("default.userTable", predicate,
          historyRetentionPeriod, latestRetentionPeriod);

      String insert = "INSERT INTO default.userTable "
          + "(userId, aString, aReal, anInt)" + " VALUES ('" + userId
          + "', 'Java producer', 3.1415926, 42)";
      producer.insert(insert);

      producer.close();

    } catch (RGMAException e) {
      System.err.println("R-GMA exception: " + e.toString());
      System.exit(1);
    }

  }
}

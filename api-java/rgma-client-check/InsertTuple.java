import org.glite.rgma.PrimaryProducer;
import org.glite.rgma.RGMAPermanentException;
import org.glite.rgma.RGMATemporaryException;
import org.glite.rgma.Storage;
import org.glite.rgma.SupportedQueries;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;

public class InsertTuple {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java InsertTuple <userId>");
            return;
        }
        try {
			PrimaryProducer pp = new PrimaryProducer(Storage.getMemoryStorage(), SupportedQueries.CH);
            String predicate = "WHERE userId = '" + args[0] + "'";
            TimeInterval historyRP = new TimeInterval(10, TimeUnit.MINUTES);
            TimeInterval latestRP = new TimeInterval(10, TimeUnit.MINUTES);
            pp.declareTable("Default.userTable", predicate, historyRP, latestRP);
            String insert = "INSERT INTO Default.userTable (userId, aString, aReal, anInt)" +
                   " VALUES ('" + args[0] + "', 'Java producer', 3.1415926, 42)";
            pp.insert(insert);
            pp.close();
        } catch (RGMATemporaryException e) {
            System.err.println("RGMATemporaryException: " + e.getMessage());
        } catch (RGMAPermanentException e) {
        	System.err.println("RGMAPermanentException: " + e.getMessage());

        }
    }
}

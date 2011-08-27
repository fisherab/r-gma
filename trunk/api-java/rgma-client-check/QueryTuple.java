import org.glite.rgma.Consumer;
import org.glite.rgma.QueryType;
import org.glite.rgma.RGMAPermanentException;
import org.glite.rgma.RGMATemporaryException;
import org.glite.rgma.TimeInterval;
import org.glite.rgma.TimeUnit;
import org.glite.rgma.TupleSet;

public class QueryTuple {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java QueryTuple <userId>");
            return;
        }
        try {
        	String query = "SELECT aString FROM Default.userTable WHERE userId='" + args[0] + "'";
            Consumer c = new Consumer(query, QueryType.H, new TimeInterval(10, TimeUnit.SECONDS));
            int numResults = 0;
            TupleSet rs = null;
            do {
            	try {
            		Thread.sleep(1000);
            	} catch (InterruptedException e) {}
            	rs = c.pop(50);
            	numResults += rs.getData().size();
            } while (! rs.isEndOfResults() );
            c.close();
            if (numResults != 1) {
            	System.err.println(numResults + " tuples returned rather than 1");
            }
        } catch (RGMATemporaryException e) {
            System.err.println("RGMATemporaryException: " + e.getMessage());
        } catch (RGMAPermanentException e) {
        	System.err.println("RGMAPermanentException: " + e.getMessage());
		}
    }
}

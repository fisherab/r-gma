import org.glite.rgma.RGMAException;
import org.glite.rgma.Schema;

public class CreateTableExample {
  public static void main(String[] args) {

    String vdb = "default";
    
    String create = "create table userTable (userId VARCHAR(255) "
        + "NOT NULL PRIMARY KEY, aString VARCHAR(255), "
        + "aReal REAL, anInt INTEGER)";
    
    java.util.List<java.lang.String> rules = 
        new java.util.LinkedList<java.lang.String>();
    rules.add("::RW");

    try {
      Schema schema = new Schema(vdb);
      schema.createTable(create, rules);

    } catch (RGMAException e) {
      System.err.println("R-GMA exception: " + e.toString());
      System.exit(1);
    }

  }
}

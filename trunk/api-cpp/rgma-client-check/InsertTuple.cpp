#include "rgma/RGMAException.h"
#include "rgma/TimeInterval.h"
#include "rgma/Storage.h"
#include "rgma/TimeUnit.h"
#include "rgma/PrimaryProducer.h"
#include "rgma/SupportedQueries.h"

#include <string>
#include <iostream>

using namespace glite::rgma;

int main(int argc, char* argv[]) {
  if (argc != 2)  {
    std::cerr<< "Usage: " << argv[0] << " <userId>" << std::endl;
    return 0;
  }

  try {

    TimeInterval terminationInterval(10, TimeUnit::MINUTES);
    PrimaryProducer pp (Storage::MEMORY, SupportedQueries::CH);

    std::string userTable("Default.userTable");
    std::string args(argv[1]);
    std::string predicate("WHERE userId = '" + args + "'");
    TimeInterval historyRP(10, TimeUnit::MINUTES);
    TimeInterval latestRP(10, TimeUnit::MINUTES);
    pp.declareTable(userTable,  predicate, historyRP, latestRP);

    std::string insert;
    insert.append("INSERT INTO Default.userTable (userId, aString, aReal, anInt)");
    insert.append(" VALUES ('");
    insert.append(args);
    insert.append("', 'C++ producer', 3.1415926, 42)");
    pp.insert(insert);
    pp.close();
  }

  catch (RGMAException rgmae) {
    std::cerr << "R-GMA application error in PrimaryProducer: " << rgmae.getMessage() << std::endl;
  }

  return 0;
}

#include "rgma/PrimaryProducer.h"
#include "rgma/RGMAException.h"
#include "rgma/Storage.h"
#include "rgma/SupportedQueries.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"

#include <string>
#include <iostream>

using glite::rgma::PrimaryProducer;
using glite::rgma::RGMAException;
using glite::rgma::Storage;
using glite::rgma::SupportedQueries;
using glite::rgma::TimeInterval;
using glite::rgma::TimeUnit;

int main(int argc, char* argv[])
{

   if (argc != 2)
   {
      std::cout<< "Usage: PrimaryProducerExample <userId>\n"<< std::endl;
      exit(1);
   }

   std::string args(argv[1]);

   try
   {
      PrimaryProducer pp (Storage::MEMORY, SupportedQueries::C);

      std::string table("default.userTable");
      std::string predicate("WHERE userId = '" + args + "'");
      TimeInterval historyRetentionPeriod(60, TimeUnit::MINUTES);
      TimeInterval latestRetentionPeriod(60, TimeUnit::MINUTES);
      pp.declareTable(table, predicate, historyRetentionPeriod,
            latestRetentionPeriod);

      std::string insert;
      insert.append("INSERT INTO default.userTable (userId, aString, ");
      insert.append("aReal, anInt) VALUES ('");
      insert.append(args);
      insert.append("', 'C++ producer', 3.1415926, 42)");
      pp.insert(insert);

      pp.close();
   }

   catch (RGMAException e)
   {
      std::cerr << "R-GMA exception: " << e.getMessage() << std::endl;
   }

}

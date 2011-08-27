#include "rgma/PrimaryProducer.h"
#include "rgma/RGMAException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/Storage.h"
#include "rgma/SupportedQueries.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"

#include <string>
#include <iostream>
#include <sstream>

using glite::rgma::PrimaryProducer;
using glite::rgma::RGMAException;
using glite::rgma::RGMAPermanentException;
using glite::rgma::RGMATemporaryException;
using glite::rgma::Storage;
using glite::rgma::SupportedQueries;
using glite::rgma::TimeInterval;
using glite::rgma::TimeUnit;

int main(int argc, char* argv[])
{

   if (argc != 2)
   {
      std::cerr << "Usage: " << argv[0] << " <userId>" << std::endl;
      exit(1);
   }

   std::string userId(argv[1]);
   PrimaryProducer* producer = 0;

   try
   {
      TimeInterval historyRetentionPeriod(50, TimeUnit::MINUTES);
      TimeInterval latestRetentionPeriod(25, TimeUnit::MINUTES);

      while(!producer)
      {
         try
         {
            producer = new PrimaryProducer(Storage::MEMORY, SupportedQueries::C);
         }
         catch (RGMATemporaryException te)
         {
            std::cerr << "RGMATemporaryException: " << te.getMessage() << std::endl;
            sleep(60);
         }
      }

      std::string predicate("WHERE userId = '" + userId + "'");
      bool tableDeclared = false;

      while (!tableDeclared)
      {
         try
         {
            producer->declareTable("default.userTable", predicate,
                  historyRetentionPeriod,
                  latestRetentionPeriod);
            tableDeclared = true;
         }
         catch (RGMATemporaryException te)
         {
            std::cerr << "RGMATemporaryException: " << te.getMessage() << std::endl;
            sleep(60);
         }
      }

      int data = 0;
      std::stringstream insert;

      while (true)
      {
         try
         {
            insert.str("");
            insert << "INSERT INTO default.userTable (userId, aString, "
            << "aReal, anInt) VALUES('" << userId
            << "', 'resilient CPP producer', 0.0, " << data << ")";
            producer->insert(insert.str());
            std::cout << insert.str() << std::endl;
            data++;
            sleep(30);
         }
         catch (RGMATemporaryException te)
         {
            std::cerr << "RGMATemporaryException: " << te.getMessage() << std::endl;
            sleep(60);
         }
      }
   }
   
   catch (RGMAException pe)
   {
      std::cerr << "RGMAPermanentException: " << pe.getMessage() << std::endl;
      delete producer;
      producer = 0;
      exit(1);
   }

}

#include "rgma/RGMAException.h"
#include "rgma/RGMAService.h"
#include "rgma/SecondaryProducer.h"
#include "rgma/Storage.h"
#include "rgma/SupportedQueries.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"

#include <string>
#include <iostream>

using glite::rgma::RGMAException;
using glite::rgma::RGMAService;
using glite::rgma::SecondaryProducer;
using glite::rgma::Storage;
using glite::rgma::SupportedQueries;
using glite::rgma::TimeInterval;
using glite::rgma::TimeUnit;

int main(int argc, char* argv[])
{

   SecondaryProducer* sp = 0;

   try
   {
      std::string location("cppExample");
      Storage storage = Storage(location);
      sp = new SecondaryProducer(storage, SupportedQueries::CL);

      std::string predicate("");
      TimeInterval historyRetentionPeriod(2, TimeUnit::HOURS);
      sp->declareTable("default.userTable", predicate, historyRetentionPeriod);

      TimeInterval terminationInterval = RGMAService::getTerminationInterval();

      while(true)
      {
         sp->showSignOfLife();
         sleep(terminationInterval.getValueAs(TimeUnit::SECONDS) / 3);
      }

   }
   catch (RGMAException e)
   {
      std::cerr << "ERROR: "<< e.getMessage() << std::endl;
   }
   catch (...)
   {
      std::cerr << "ERROR: Cuaght unexpected error" << std::endl;
   }

   if (sp)
   {
      try
      {
         sp->close();
      }
      catch (RGMAException e)
      {
      }
   }

   delete sp;
   sp = 0;

}

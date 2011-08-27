#include "rgma/Consumer.h"
#include "rgma/QueryTypeWithInterval.h"
#include "rgma/RGMAException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"
#include "rgma/TupleSet.h"

#include <string>
#include <iostream>

using glite::rgma::Consumer;
using glite::rgma::QueryTypeWithInterval;
using glite::rgma::RGMAException;
using glite::rgma::RGMAPermanentException;
using glite::rgma::RGMATemporaryException;
using glite::rgma::TimeInterval;
using glite::rgma::TimeUnit;
using glite::rgma::TupleSet;

int main(int argc, char* argsv[])
{
   Consumer* consumer = 0;
   std::string select("SELECT userId, aString, aReal, anInt FROM "
      "default.userTable");

   try
   {
      TimeInterval historyPeriod(30, TimeUnit::SECONDS);

      while (!consumer)
      {
         try
         {
            consumer = new Consumer(select, QueryTypeWithInterval::C, historyPeriod);
         }
         catch (RGMATemporaryException te)
         {
            std::cerr << "RGMATemporaryException: " << te.getMessage() << std::endl;
            sleep(60);
         }
      }

      while (true)
      {
         try
         {
            TupleSet tupleSet;
            consumer->pop(2000, tupleSet);
            if(tupleSet.begin() == tupleSet.end())
            {
               sleep(2);
            }
            else
            {

               TupleSet::const_iterator tuple = tupleSet.begin();
               while (tuple != tupleSet.end())
               {
                  std::cout << "userId=" << tuple->getString(0) << ", ";
                  std::cout << "aString=" << tuple->getString(1) << ", ";
                  std::cout << "aReal=" << tuple->getFloat(2) << ", ";
                  std::cout << "anInt=" << tuple->getInt(3) << std::endl;
                  ++tuple;
               }

            }
            std::string warning = tupleSet.getWarning();
            if (warning.length()> 0)
            {
               std::cout << "WARNING: " << tupleSet.getWarning()
               << std::endl;
            }
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
      delete consumer;
      consumer = 0;
      exit(1);
   }

}

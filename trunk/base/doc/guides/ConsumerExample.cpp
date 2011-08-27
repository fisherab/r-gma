#include "rgma/Consumer.h"
#include "rgma/QueryTypeWithInterval.h"
#include "rgma/RGMAException.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"
#include "rgma/TupleSet.h"

#include <string>
#include <iostream>

using glite::rgma::Consumer;
using glite::rgma::QueryTypeWithInterval;
using glite::rgma::RGMAException;
using glite::rgma::TimeInterval;
using glite::rgma::TimeUnit;
using glite::rgma::TupleSet;

int main(int argc, char* argsv[])
{

   try
   {
      TimeInterval historyPeriod(10, TimeUnit::MINUTES);
      TimeInterval timeout(5, TimeUnit::MINUTES);
      std::string select("SELECT userId, aString, aReal, "
            "anInt, RgmaTimestamp FROM "
            "default.userTable");
      Consumer c(select, QueryTypeWithInterval::C, historyPeriod, timeout);

      bool endOfResults = false;
      while (!endOfResults)
      {
         TupleSet tupleSet;
         c.pop(2000, tupleSet);
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
         endOfResults = tupleSet.isEndOfResults();
      }

      c.close();
   }

   catch (RGMAException e)
   {
      std::cerr << "R-GMA application error in Consumer."<< std::endl;
      std::cout << e.getMessage() << std::endl;
   }

}

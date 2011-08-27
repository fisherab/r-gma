#include "rgma/Schema.h"
#include "rgma/RGMAException.h"

#include <string>
#include <iostream>

using glite::rgma::Schema;
using glite::rgma::RGMAException;

int main(int argc, char* argsv[])
{
   std::string vdb("default");
   
   std::string create("create table userTable (userId VARCHAR(255) NOT NULL "
      "PRIMARY KEY, aString VARCHAR(255), aReal REAL, "
      "anInt INTEGER)");
   
   std::vector<std::string> rules;
   rules.push_back("::RW");

   try
   {
      Schema schema(vdb);
      schema.createTable(create, rules);
   }

   catch (RGMAException e)
   {
      std::cerr << "R-GMA exception: " << e.getMessage() << std::endl;
   }

}

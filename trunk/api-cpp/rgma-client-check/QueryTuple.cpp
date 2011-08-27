#include "rgma/Consumer.h"
#include "rgma/QueryType.h"
#include "rgma/TupleSet.h"
#include "rgma/TimeInterval.h"
#include "rgma/TimeUnit.h"
#include "rgma/RGMAException.h"

#include <string>
#include <iostream>

using namespace glite::rgma;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <userId>" << std::endl;
        return 0;
    }

    try {
        std::string select;
        select.append("SELECT aString FROM Default.userTable WHERE userId='");
        select.append(argv[1]);
        select.append("'");

        Consumer c(select, QueryType::H, TimeInterval(10, TimeUnit::SECONDS));

        int numResults = 0;
        TupleSet resultSet;
        do {
            sleep(1);
            c.pop(50, resultSet);
            TupleSet::const_iterator iter = resultSet.begin();
            while (iter != resultSet.end()) {
                numResults++;
                iter++;
            }
        } while (!resultSet.isEndOfResults());
        c.close();

        if (numResults != 1) {
            fprintf(stderr, "%d tuples returned rather than 1\n", numResults);
        }

    } catch (RGMAException rgmae) {
        std::cerr << "R-GMA application error in Consumer: " << rgmae.getMessage() << std::endl;
    }

    return 0;
}

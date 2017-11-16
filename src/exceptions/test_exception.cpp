/**
 * @author Justin Snyder
 */

#include "test_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

    TestFailedException::TestFailedException(std::string testName)
            : BadgerDbException(""){
        std::stringstream ss;
        ss << "Test Failed: " << testName;
        message_.assign(ss.str());
    }
}
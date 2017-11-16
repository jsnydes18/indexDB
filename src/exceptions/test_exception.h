/**
 * @author Justin Snyder
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a test has failed. More info can be gained from test output.
 */
    class TestFailedException : public BadgerDbException {
    public:
        /**
         * Constructs a TestFailedException exception
         */
        explicit TestFailedException(std::string testName);
    };
}
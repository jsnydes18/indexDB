/**
 * @author Justin Snyder
 */

#include "test_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

    HeaderPageNumberIncorrect::HeaderPageNumberIncorrect(PageId headerPageId)
            : BadgerDbException(""){
        std::stringstream ss;
        ss << "Header Page Number: " << headerPageId;
        message_.assign(ss.str());
    }

    RootPageNumberIncorrect::RootPageNumberIncorrect(PageId rootPageId)
            : BadgerDbException(""){
        std::stringstream ss;
        ss << "Root Page Number immediately after Index Creation: " << rootPageId;
        message_.assign(ss.str());
    }

    RelationNameIncorrect::RelationNameIncorrect(std::string relationName)
            : BadgerDbException(""){
        std::stringstream ss;
        ss << "Relation Name found: " << relationName;
        message_.assign(ss.str());
    }
}
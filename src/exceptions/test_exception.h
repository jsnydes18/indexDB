/**
 * @author Justin Snyder
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a header page has a PageId that is not 1.
 */
    class HeaderPageNumberIncorrect : public BadgerDbException {
    public:
        /**
         * Constructs a HeaderPageNumberIncorrect exception
         */
        explicit HeaderPageNumberIncorrect(PageId headerPageId);
    };

/**
 * @brief An exception that is thrown when a root page has a PageId that is not 2 immediately after Index creation.
 */
    class RootPageNumberIncorrect : public BadgerDbException {
    public:
        /**
         * Constructs a RootPageNumberIncorrect
         */
        explicit RootPageNumberIncorrect(PageId rootPageId);
    };

/**
 * @brief An exception that is thrown when an existing index does not have the expected Relation Name
 */
    class RelationNameIncorrect : public BadgerDbException {
    public:
        /**
         * Constructs a RelationNameIncorrect
         */
        explicit RelationNameIncorrect(std::string relationName);
    };
}
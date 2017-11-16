/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/test_exception.h"


//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType) {
        //Set Index Members
        bufMgr = bufMgrIn;
        this->attrByteOffset = attrByteOffset;
        attributeType = attrType;
        nodeOccupancy = INTARRAYNONLEAFSIZE;
        leafOccupancy = INTARRAYLEAFSIZE;
        //	These Members to be updated as needed
        scanExecuting = false;
        nextEntry = 0;
        currentPageNum = 0;
        currentPageData = nullptr;
        lowValInt = 0;
        lowValDouble = 0;
        highValInt = 1;
        highValDouble = 1;
        ///Currently Using Less Than and Greater Than Equal To
        lowOp = LT;
        highOp = GTE;


        //Set Index File Name
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        outIndexName = idxStr.str();

        //Create Index File
        try {
            BlobFile indexFile(outIndexName, false);
            file = &indexFile;
            headerPageNum = 1;

            //Test Scenario: Test Opening an existing index file
            if (relationName == "test") {
                std::cout << "Opened Test Index File" << std::endl;
                return;
            }

        } catch (FileNotFoundException e) {
            BlobFile indexFile(outIndexName, true);

            file = &indexFile;
            //	Meta Info Page
            PageId metaPageId;
            PageId &metaRef = metaPageId;

            //Allocate Page from file
            Page metaPage = file->allocatePage(metaRef);
            Page *metaPtr = &metaPage;

            //Read Page into Buffer
            bufMgr->readPage(file, metaPageId, metaPtr);
            IndexMetaInfo *metaInfo = (IndexMetaInfo *) metaPtr;


            //Set Members
            strcpy(metaInfo->relationName, relationName.c_str());
            metaInfo->attrType = attrType;
            metaInfo->attrByteOffset = attrByteOffset;

            //Update File Members
            headerPageNum = metaPageId;

            ///Could Unpin Meta Page here, but it will be updated as soon as root is created


            // 	Root Node
            PageId rootPageId;
            PageId &rootRef = rootPageId;

            //Allocate Page for root node
            Page rootPage = file->allocatePage(rootRef);
            Page *rootPtr = &rootPage;

            //Read Page into Buffer
            bufMgr->readPage(file, rootPageId, rootPtr);
            LeafNodeInt *rootNode = (LeafNodeInt *) rootPtr;

            //Update meta page
            metaInfo->rootPageNo = rootPageId;

            //Unpin Page from Buffer and Mark as Dirty
            bufMgr->unPinPage(file, headerPageNum, true);

            //Update File Members
            rootPageNum = rootPageId;
            //file = &indexFile;

            ///Could Unpin Root Page here, but more than likely going to be updating it very soon

            //Test Scenario: Test Index Initial Creation
            if (relationName == "test") {
                bufMgr->unPinPage(file, rootPageNum, true);
                //Ensure file is updated for next test
                bufMgr->flushFile(file);
                return;
            }

            //Scan Through Relation and Build Index
            //Begin scanning through the relation
            FileScan currScan(relationName, bufMgr);

            //Scan relation to get initial record
            RecordId currRecordId;
            RecordId &currRecordIdRef = currRecordId;
            currScan.scanNext(currRecordIdRef);

            //Give rootPage initial record information
            std::string initialRecordStr = currScan.getRecord();
            uRECORD *initialRecord = (uRECORD *) initialRecordStr.c_str();
            rootNode->keyArray[0] = initialRecord->i;
            rootNode->ridArray[0] = currRecordId;


            ///Filling Root Node past full to guarentee that root node is always a NonLeafNode
            for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                //Look at next record
                currScan.scanNext(currRecordIdRef);
                std::string currRecStr = currScan.getRecord();
                uRECORD *currRecPtr = (uRECORD *) currRecStr.c_str();

                //Temp Variable for help with inserting
                int insertAt;
                for (int m = 0; m < INTARRAYLEAFSIZE; m++) {
                    if (currRecPtr->i < rootNode->keyArray[m] && m == 0) {
                        insertAt = 0;
                    } else if (currRecPtr->i > rootNode->keyArray[m] && currRecPtr->i < rootNode->keyArray[m + 1]) {
                        insertAt = m + 1;
                    } else if (currRecPtr->i > rootNode->keyArray[m] && m == INTARRAYLEAFSIZE - 2) {
                        insertAt = INTARRAYLEAFSIZE - 1;
                    }
                }

                //Shift all values in array over if necessary
                if (insertAt != INTARRAYLEAFSIZE - 1) {
                    for (int j = INTARRAYLEAFSIZE - 1; j >= insertAt; j--) {
                        rootNode->keyArray[j] = rootNode->keyArray[j - 1];
                        rootNode->ridArray[j] = rootNode->ridArray[j - 1];
                    }
                }

                //Insert new key,recordId pair
                rootNode->keyArray[insertAt] = currRecPtr->i;
                rootNode->ridArray[insertAt] = currRecordId;
            }

            //Root is Full
            //Create a Sibling Node
            PageId sibId;
            PageId &sibIdRef = sibId;

            Page sibPage = file->allocatePage(sibIdRef);
            std::cout << "Sibling Page ID: " << sibId << std::endl;

            //Read Sibling Node into buffer
            Page *sibPtr = &sibPage;
            bufMgr->readPage(file, sibId, sibPtr);
            LeafNodeInt *sibNodePtr = (LeafNodeInt *) sibPtr;

            //Set New Page as a sibling
            rootNode->rightSibPageNo = sibId;

            //Give Sibling Upper Half of values
            for (int k = INTARRAYLEAFSIZE / 2; k < INTARRAYLEAFSIZE; k++){
                sibNodePtr->keyArray[k - INTARRAYLEAFSIZE / 2] = rootNode->keyArray[k];
                sibNodePtr->ridArray[k - INTARRAYLEAFSIZE / 2] = rootNode->ridArray[k];
                rootNode->keyArray[k] = 0;
            }

            //Create new Root Node
            PageId newRootId;
            PageId &newRootRef = newRootId;
            Page newRootPage = file->allocatePage(newRootRef);
            std::cout << "New Root Page Id: " << newRootId << std::endl;
            Page *newRootPtr = &newRootPage;
            bufMgr->readPage(file, newRootId, newRootPtr);
            NonLeafNodeInt *newRootNode = (NonLeafNodeInt *) newRootPtr;

            //Update new Root Node
            newRootNode->keyArray[0] = sibNodePtr->keyArray[0];
            newRootNode->pageNoArray[0] = rootPageId;
            newRootNode->pageNoArray[1] = sibId;

            //Update BTree Member
            rootPageNum = newRootId;

            ///Unpinning everything here since we shift our implementation
            bufMgr->unPinPage(file, newRootId, true);
            bufMgr->unPinPage(file, sibId, true);
            bufMgr->unPinPage(file, rootPageId, true);



            ///NEW TEST CASE HERE. MAKE SURE THAT TREE IS CORRECT NOW!

            /*
            //Begin inserting entries and building the B+ tree
            while (true) {
                try {
                    currScan.scanNext(currRecordIdRef);
                    std::string currRecord = currScan.getRecord();
                    uRECORD *record = (uRECORD *) currRecord.c_str();

                    int *key = &record->i;
                    insertEntry(key, currRecordId);

                } catch (EndOfFileException e) {
                    //End of Relation Found
                    return;
                }

                //Access the page via casted pointer
            }
            */
        }
        return;
    }


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        //Read Root Node Into Buffer
        //Cast Page Pointer to NonLeafNode Pointer
        //Scan through Key Values to find correct child
        //Found PageId of correct Child
        //Unpin Root Node Page
        //Is Root level = 1?
        //Read Child PageId into buffer
        //Cast Page Pointer Appropiately
        //If at leaf, insert
        //If not at leaf, continue searching
    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {

    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {

    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {

    }



    ///Following functions used for testing
    const PageId BTreeIndex::getRootPageNum() {
        return rootPageNum;
    }

    const PageId BTreeIndex::getHeaderPageNum() {
        return headerPageNum;
    }

    const std::string BTreeIndex::getRelName() {
        std::string relationName;
        Page header;
        Page *headerPtr = &header;

        bufMgr->readPage(file, headerPageNum, headerPtr);
        IndexMetaInfo *ptr = (IndexMetaInfo*) headerPtr;
        relationName = ptr->relationName;

        bufMgr->unPinPage(file headerPageNum, false);

        return relationName;
    }

}

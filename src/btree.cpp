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
#include "exceptions/bad_scanrange_exception.h"
#include <vector>


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
        lowValDouble = 0;
        highValDouble = 0;

        //Set Index File Name
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        outIndexName = idxStr.str();

        //Create Index File
        try {
            file = new BlobFile(outIndexName, false);
            headerPageNum = 1;

            Page headerPage;
            Page *headerPagePtr;

            bufMgr->readPage(file, headerPageNum, headerPagePtr);
            IndexMetaInfo *metaPtr = (IndexMetaInfo *) headerPagePtr;
            rootPageNum = metaPtr->rootPageNo;
            bufMgr->unPinPage(file, headerPageNum, false);

        } catch (FileNotFoundException e) {
            file = new BlobFile(outIndexName, true);

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
            for (int i = 0; i < INTARRAYLEAFSIZE - 1; i++) {
                rootNode->keyArray[i] = -1;
            }

            //Update meta page
            metaInfo->rootPageNo = rootPageId;

            ///Should Unpin Header Here?
            //Unpin Page from Buffer and Mark as Dirty
            //bufMgr->unPinPage(file, headerPageNum, true);

            //Update File Members
            rootPageNum = rootPageId;

            ///Could Unpin Root Page here, but more than likely going to be updating it very soon

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
                    if (rootNode->keyArray[m] == -1) {
                        insertAt = m;
                        break;
                    } else if (currRecPtr->i < rootNode->keyArray[m]) {
                        insertAt = m;
                        break;
                    }
                }

                //Shift all values in array over if necessary
                if (insertAt != INTARRAYLEAFSIZE - 1) {
                    for (int j = INTARRAYLEAFSIZE - 1; j > insertAt; j--) {
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

            //Read Sibling Node into buffer
            Page *sibPtr = &sibPage;
            bufMgr->readPage(file, sibId, sibPtr);
            LeafNodeInt *sibNodePtr = (LeafNodeInt *) sibPtr;
            for (int i = 0; i < INTARRAYLEAFSIZE - 1; i++) {
                sibNodePtr->keyArray[i] = -1;
            }

            //Set New Page as a sibling
            rootNode->rightSibPageNo = sibId;

            //Give Sibling Upper Half of values
            for (int k = INTARRAYLEAFSIZE / 2; k < INTARRAYLEAFSIZE; k++) {
                sibNodePtr->keyArray[k - INTARRAYLEAFSIZE / 2] = rootNode->keyArray[k];
                sibNodePtr->ridArray[k - INTARRAYLEAFSIZE / 2] = rootNode->ridArray[k];
                rootNode->keyArray[k] = -1;
            }

            //Create new Root Node
            PageId newRootId;
            PageId &newRootRef = newRootId;
            Page newRootPage = file->allocatePage(newRootRef);
            Page *newRootPtr = &newRootPage;

            //Read new Root Node into buffer
            bufMgr->readPage(file, newRootId, newRootPtr);
            NonLeafNodeInt *newRootNode = (NonLeafNodeInt *) newRootPtr;
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                newRootNode->keyArray[i] = -1;
            }

            //Update new Root Node
            newRootNode->keyArray[0] = sibNodePtr->keyArray[0];

            newRootNode->pageNoArray[0] = rootPageId;
            newRootNode->pageNoArray[1] = sibId;
            newRootNode->level = 1;

            //Update BTree Member
            rootPageNum = newRootId;

            //Update header page
            metaInfo->rootPageNo = newRootId;
            bufMgr->unPinPage(file, headerPageNum, true);

            ///Unpinning everything here since we shift our implementation
            bufMgr->unPinPage(file, newRootId, true);
            bufMgr->unPinPage(file, sibId, true);
            bufMgr->unPinPage(file, rootPageId, true);
            bufMgr->flushFile(file);

            //Begin inserting entries and building the B+ tree
            while (true) {
                try {
                    currScan.scanNext(currRecordIdRef);
                    std::string currRecord = currScan.getRecord();
                    uRECORD *record = (uRECORD *) currRecord.c_str();

                    int sample = record->i;
                    int *key = &sample;

                    insertEntry(key, currRecordId);

                } catch (EndOfFileException e) {
                    //End of Relation Found
                    return;
                }

                //Access the page via casted pointer
            }
        }
        return;
    }


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

    BTreeIndex::~BTreeIndex() {

        ///Clean Up State Variables?

        //Unpin All Pages From the file
        try {
            bufMgr->unPinPage(file, currentPageNum, false);
        } catch (BadgerDbException e) {}

        //Flush The File
        bufMgr->flushFile(file);

        //Close the file
        file->~File();
        //file = NULL;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        PageId currPageId = rootPageNum;
        Page *currPage;
        int newKey = *(int *) key;

        //Start At Root Node
        bufMgr->readPage(file, currPageId, currPage);
        NonLeafNodeInt *currNode = (NonLeafNodeInt *) currPage;

        //Array for keeping track of which nodes have been visited
        int treeHeight = currNode->level + 1;
        PageId nodesScanned[treeHeight];

        //Add the root to the list
        nodesScanned[treeHeight - 1] = currPageId;

        //Scan through tree nodes until leaf parent is found
        PageId nextNode;
        bool leafParentFound = false;
        while (!leafParentFound) {
            //Find Index of Correct Child
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                if (currNode->keyArray[i] > newKey) {
                    nextNode = currNode->pageNoArray[i];
                    break;
                }
                if (currNode->keyArray[i] == -1) {
                    nextNode = currNode->pageNoArray[i];
                    break;
                }
                if (currNode->keyArray[i] == newKey) {
                    nextNode = currNode->pageNoArray[i + 1];
                    break;
                }
                if (i == INTARRAYNONLEAFSIZE - 1) {
                    nextNode = currNode->pageNoArray[i + 1];
                    break;
                }
            }

            //If this node is at level 1, then it's child is a leaf
            if (currNode->level == 1) {
                leafParentFound = true;
            }

            //unpin the current page
            bufMgr->unPinPage(file, currPageId, false);

            if (!leafParentFound) {
                //Move the pointer to the next node
                currPageId = nextNode;
                bufMgr->readPage(file, currPageId, currPage);
                currNode = (NonLeafNodeInt *) currPage;

                //Add this node to the list of nodes scanned
                nodesScanned[currNode->level] = currPageId;
            }
        }

        //Next Node is the leaf node
        PageId leafId = nextNode;
        currPageId = nextNode;
        bufMgr->readPage(file, currPageId, currPage);
        LeafNodeInt *leafNode = (LeafNodeInt *) currPage;
        nodesScanned[0] = currPageId;

        int insertAt = -1;

        //Leaf Node has room, insert it and return
        if (leafNode->keyArray[INTARRAYLEAFSIZE-1] == -1) {
            for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                if (leafNode->keyArray[i] == -1) {
                    insertAt = i;
                    break;
                }
                if (leafNode->keyArray[i] > newKey) {
                    insertAt = i;
                    break;
                }
            }
            for (int i = INTARRAYLEAFSIZE - 1; i > insertAt; i--) {
                leafNode->keyArray[i] = leafNode->keyArray[i - 1];
                leafNode->ridArray[i] = leafNode->ridArray[i - 1];
            }
            leafNode->keyArray[insertAt] = newKey;
            leafNode->ridArray[insertAt] = rid;
            bufMgr->unPinPage(file, currPageId, true);
            return;
        }

        //Leaf Node is full, splits required
        PageId sibId;
        PageId &sibRef = sibId;
        Page sibPage = file->allocatePage(sibRef);

        //Read Sibling Page into buffer
        Page *sibPagePtr = &sibPage;
        bufMgr->readPage(file, sibId, sibPagePtr);
        LeafNodeInt *sibPtr = (LeafNodeInt *) sibPagePtr;
        for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
            sibPtr->keyArray[i] = -1;
        }

        //Give Sibling Upper Half of values
        for (int k = INTARRAYLEAFSIZE / 2; k < INTARRAYLEAFSIZE; k++) {
            sibPtr->keyArray[k - INTARRAYLEAFSIZE / 2] = leafNode->keyArray[k];
            sibPtr->ridArray[k - INTARRAYLEAFSIZE / 2] = leafNode->ridArray[k];
            leafNode->keyArray[k] = -1;
        }

        if(newKey < sibPtr->keyArray[0]){
            //Insert into left node
            for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                if (leafNode->keyArray[i] == -1) {
                    insertAt = i;
                    break;
                }
                if (leafNode->keyArray[i] > newKey) {
                    insertAt = i;
                    break;
                }
            }

            for (int i = INTARRAYLEAFSIZE - 1; i > insertAt; i--) {
                leafNode->keyArray[i] = leafNode->keyArray[i - 1];
                leafNode->ridArray[i] = leafNode->ridArray[i - 1];
            }
            leafNode->keyArray[insertAt] = newKey;
            leafNode->ridArray[insertAt] = rid;
        }
        else {
            //Insert into right node
            for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                if (sibPtr->keyArray[i] == -1) {
                    insertAt = i;
                    break;
                }
                if (sibPtr->keyArray[i] > newKey) {
                    insertAt = i;
                    break;
                }
            }

            for (int i = INTARRAYLEAFSIZE - 1; i > insertAt; i--) {
                sibPtr->keyArray[i] = sibPtr->keyArray[i - 1];
                sibPtr->ridArray[i] = sibPtr->ridArray[i - 1];
            }
            sibPtr->keyArray[insertAt] = newKey;
            sibPtr->ridArray[insertAt] = rid;
        }

        //Update Right Sibling Pointers
        sibPtr->rightSibPageNo = leafNode->rightSibPageNo;
        leafNode->rightSibPageNo = sibId;
        bufMgr->unPinPage(file, leafId, true);

        //Grab Key to be pushed up then unpin pages
        int upKey = sibPtr->keyArray[0];
        bufMgr->unPinPage(file, sibId, true);

        for (int i = 1; i <= treeHeight; i++) {
            if(i == treeHeight){
                //Nothing else to scan, make a new root
                PageId newRootId;
                PageId &newRootRef = newRootId;
                Page newRootPage = file->allocatePage(newRootRef);
                Page *newRootPagePtr = &newRootPage;
                bufMgr->readPage(file, newRootId, newRootPagePtr);
                NonLeafNodeInt *rootPtr = (NonLeafNodeInt*) newRootPagePtr;

                rootPtr->level = treeHeight;
                rootPtr->keyArray[0] = upKey;
                rootPtr->pageNoArray[0] = currPageId;
                rootPtr->pageNoArray[1] = sibId;

                bufMgr->unPinPage(file, newRootId, true);

                Page *header;
                bufMgr->readPage(file, headerPageNum, header);

                IndexMetaInfo *headerPtr = (IndexMetaInfo*) header;
                headerPtr->rootPageNo = newRootId;
                bufMgr->unPinPage(file, headerPageNum, true);

                rootPageNum = newRootId;
                break;
            }
            //Read in Node Parent
            currPageId = nodesScanned[i];

            bufMgr->readPage(file, currPageId, currPage);
            currNode = (NonLeafNodeInt *) currPage;


            //Find where the new key should be put
            for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                if (currNode->keyArray[i] == -1) {
                    insertAt = i;
                    break;
                }
                if (currNode->keyArray[i] > newKey) {
                    insertAt = i;
                    break;
                }
            }

            //Check If There is Room in this node
            if (currNode->keyArray[INTARRAYNONLEAFSIZE - 1] == -1) {
                //Move Values over
                for (int i = INTARRAYNONLEAFSIZE - 1; i > insertAt; i--) {
                    currNode->keyArray[i] = currNode->keyArray[i - 1];
                    currNode->pageNoArray[i + 1] = currNode->pageNoArray[i];
                }

                //Insert Key
                currNode->keyArray[insertAt] = upKey;
                currNode->pageNoArray[insertAt + 1] = sibId;

                //Unpin The Page and done.
                bufMgr->unPinPage(file, currPageId, true);
                break;
            } else {
                PageId upSibId;
                PageId &upSibRef = upSibId;
                Page upSibPage = file->allocatePage(upSibRef);
                Page *upSibPagePtr = &upSibPage;
                bufMgr->readPage(file, upSibId, upSibPagePtr);
                NonLeafNodeInt *upSibPtr = (NonLeafNodeInt *) upSibPagePtr;
                for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
                    upSibPtr->keyArray[i] = -1;
                }

                //Fill Up The Sibling Page
                for (int i = INTARRAYNONLEAFSIZE / 2; i < INTARRAYNONLEAFSIZE - 1; i++) {
                    if (i == INTARRAYNONLEAFSIZE / 2) {
                        if (i == insertAt) {
                            upSibPtr->pageNoArray[0] = sibId;
                        } else {
                            upSibPtr->pageNoArray[0] = currNode->pageNoArray[i + 1];
                        }
                    }
                    if (i == insertAt) {
                        upSibPtr->keyArray[i] = upKey;
                        upSibPtr->pageNoArray[i + 1] = sibId;
                    } else {
                        upSibPtr->keyArray[i - INTARRAYNONLEAFSIZE / 2] = currNode->keyArray[i];
                        upSibPtr->pageNoArray[i - (INTARRAYNONLEAFSIZE / 2) + 1] = currNode->pageNoArray[i + 1];
                    }
                    currNode->keyArray[i] = -1;
                }

                if (insertAt < INTARRAYNONLEAFSIZE / 2) {
                    //Move Values over
                    for (int i = INTARRAYNONLEAFSIZE - 1; i > insertAt; i--) {
                        currNode->keyArray[i] = currNode->keyArray[i - 1];
                        currNode->pageNoArray[i + 1] = currNode->pageNoArray[i];
                    }

                    //Insert Key
                    currNode->keyArray[insertAt] = upKey;
                    currNode->pageNoArray[insertAt + 1] = sibId;
                }


                sibId = upSibId;
                upKey = upSibPtr->pageNoArray[0];
                bufMgr->unPinPage(file, upSibId, true);
                bufMgr->unPinPage(file, currPageId, true);


                continue;
            }
        }

    }

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

    const void BTreeIndex::startScan(const void *lowValParm,
                                     const Operator lowOpParm,
                                     const void *highValParm,
                                     const Operator highOpParm) {

        //Update Object Members
        scanExecuting = true;
        lowOp = lowOpParm;
        highOp = highOpParm;
        lowValInt = *(int *) lowValParm;
        highValInt = *(int *) highValParm;

        //Throw an error if something is wrong
        if (lowValInt > highValInt) {
            throw BadScanrangeException();
        }
        if (lowOp != GT && lowOp != GTE) {
            throw BadOpcodesException();
        }
        if (highOp != LT && highOp != LTE) {
            throw BadOpcodesException();
        }
        ///We Don't get negative values?
        if(lowValInt < 0){
            lowValInt = 0;
            lowOp = GTE;
        }

        //Find our starting leaf node
        //Start at root node
        Page *currPagePtr;

        bufMgr->readPage(file, rootPageNum, currPagePtr);
        NonLeafNodeInt *nonLeafPtr = (NonLeafNodeInt *) currPagePtr;

        //Update Member Functions
        currentPageNum = rootPageNum;
        currentPageData = currPagePtr;

        PageId nextNode;
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
            if (nonLeafPtr->keyArray[i] == -1) {
                //Greater than every key in node that is not full
                nextNode = nonLeafPtr->pageNoArray[i];
                break;
            }
            if (lowValInt < nonLeafPtr->keyArray[i]) {
                //Greater than every key in node up to ith key
                nextNode = nonLeafPtr->pageNoArray[i];
                break;
            }
            if (i == INTARRAYNONLEAFSIZE - 1) {
                //Greater than every key in node that is full
                nextNode = nonLeafPtr->pageNoArray[INTARRAYNONLEAFSIZE];
            }
        }
        ///Unpin Root because we're done reading it
        bufMgr->unPinPage(file, rootPageNum, false);

        //If root was level one, then we've found a leaf
        bool leafNodeFound = false;
        if (nonLeafPtr->level == 1) {
            leafNodeFound = true;
        }

        //Loop Down through NonLeafNodes to Leaves
        while (!leafNodeFound) {
            ///TODO IMPLEMENT THIS
            std::cout << "Here" << std::endl;
            leafNodeFound = true;
        }

        //Read in the leaf node
        bufMgr->readPage(file, nextNode, currPagePtr);
        LeafNodeInt *leafPtr = (LeafNodeInt *) currPagePtr;

        currentPageNum = nextNode;
        currentPageData = currPagePtr;

        //Find the starting entry in the leaf node
        for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
            if (lowValInt == leafPtr->keyArray[i]) {
                if (lowOp == GTE) {
                    nextEntry = i;
                } else if (lowOp == GT) {
                    nextEntry = i + 1;
                }
            }
        }
        ///Don't Unpin the leaf node since we're not done reading it
        return;


    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    const void BTreeIndex::scanNext(RecordId &outRid) {
        LeafNodeInt *leafPtr = (LeafNodeInt *) currentPageData;

        //Is next entry outside scan range
        if (highOp == LT && leafPtr->keyArray[nextEntry] >= highValInt) {
            //Scan End
            //endScan();
            throw IndexScanCompletedException();
        } else if (highOp == LTE && leafPtr->keyArray[nextEntry] > highValInt) {
            //Scan End
            //endScan();
            throw IndexScanCompletedException();
        }

        //If not, return the RecordId
        outRid = leafPtr->ridArray[nextEntry];

        //Move nextEntry forward
        if (leafPtr->keyArray[nextEntry + 1] == -1) {
            Page *nextPage;
            PageId nextPageId = leafPtr->rightSibPageNo;

            ///Unpin page first, to ensure room
            bufMgr->unPinPage(file, currentPageNum, false);
            bufMgr->readPage(file, nextPageId, nextPage);

            //Update Object members
            nextEntry = 0;
            currentPageNum = nextPageId;
            currentPageData = nextPage;

            return;
        }

        nextEntry++;
        return;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    const void BTreeIndex::endScan() {
        //Ensure Scan Actually Started
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }

        //Current Page should be the only page pinned for the purpose of the scan
        bufMgr->unPinPage(file, currentPageNum, false);

        //Reset variables
        scanExecuting = false;
        nextEntry = -1;
        currentPageData = NULL;
        currentPageNum = 0;
        lowValInt = 0;
        highValInt = 1;
        return;

    }


    ///Following functions used for testing
    const PageId BTreeIndex::getRootPageNum() {
        return rootPageNum;
    }

    const PageId BTreeIndex::getHeaderPageNum() {
        return headerPageNum;
    }

    const std::string BTreeIndex::getRelName() {
        Page header;
        Page *headerPtr = &header;
        std::string relationName;

        bufMgr->readPage(file, headerPageNum, headerPtr);
        IndexMetaInfo *ptr = (IndexMetaInfo *) headerPtr;
        relationName = ptr->relationName;

        bufMgr->unPinPage(file, headerPageNum, false);

        return relationName;
    }
}

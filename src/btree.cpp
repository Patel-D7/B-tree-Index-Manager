/**
 * Name: Dhruchita Patel  | Student Id: 9078238962
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <climits>
#include <stack>
#include <climits>
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
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_pinned_exception.h"


//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(
    const std::string& relationName,
    std::string& outIndexName,
    BufMgr* bufMgrIn,
    const int attrByteOffset,
    const Datatype attrType)
{

    /* Generate file name as proposed */
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();

    /* init buffer manager and set attribute */
    bufMgr = bufMgrIn;
    attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    leafOccupancy = 0;
    nodeOccupancy = 0;
    scanExecuting = false;

    IndexMetaInfo* metadata;
    Page* headerPage;
    Page* rootPage;

    try {
        /* create the file based on BlobFile as proposed and check if exists */
        /* create new index if not exist */
        file = new BlobFile(outIndexName, true);

        /* Allocate index meta info page and btree root page */
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);

        /* set meta data info for the index*/
        metadata = (IndexMetaInfo*)headerPage;
        strcpy(metadata->relationName, relationName.c_str());
        metadata->attrByteOffset = attrByteOffset;
        metadata->attrType = attrType;
        metadata->rootPageNo = rootPageNum;

        /* assuming int as proposed */
        /* set tree root */
        auto root = (NonLeafNodeInt*)rootPage;
        root->level = 1;
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
            clearNonLeafNodeAtIdx(root, i);
        }
        root->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

        /* Relation scan */
        try {
            FileScan fileScan(relationName, bufMgr);
            RecordId rid = {};
            while (true) {
                fileScan.scanNext(rid);
                /* insert data tuples into index */
                insertEntry((int*)fileScan.getRecord().c_str() + attrByteOffset, rid);
            }
        }
        catch (EndOfFileException& e) {
            /* catch EOF as proposed */
        }

        /* if the page isnt in use, unpin it */
        try {
            bufMgr->unPinPage(file, headerPageNum, true);
        }
        catch (PageNotPinnedException& e) {

        }
        try {
            bufMgr->unPinPage(file, rootPageNum, true);
        }
        catch (PageNotPinnedException& e) {

        }
    }
    catch (FileExistsException& e) { 
        /* grab the file if exists */
        file = new BlobFile(outIndexName, false);

        /* Get page number */
        headerPageNum = file->getFirstPageNo();

        /* Retrieve metadata */
        bufMgr->readPage(file, headerPageNum, headerPage);
        metadata = (IndexMetaInfo*)headerPage;

        /* Compare parameters and meta data */
        if (strcmp(metadata->relationName, relationName.c_str()) != 0
            || metadata->attrByteOffset != attrByteOffset
            || metadata->attrType != attrType) {
            try {
                /* Unpin page if not exists */
                bufMgr->unPinPage(file, headerPageNum, false);
            }
            catch (PageNotPinnedException& e) {
            }
            throw BadIndexInfoException("ERROR METADATA NOT MATCHING");
        }


        /* If metadata matches set root page for the index */
        rootPageNum = metadata->rootPageNo;

        try {
            /* Unpin header */
            bufMgr->unPinPage(file, headerPageNum, false);
        }
        catch (PageNotPinnedException& e) {

        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
BTreeIndex::~BTreeIndex()
{
    /* stop scan */
    scanExecuting = false;

    /* Unpin pages not being used */
    try {
        bufMgr->unPinPage(file, currentPageNum, false);
    }
    catch (PageNotPinnedException& e) {
    }

    /* Release buffer and delete file  */
    bufMgr->flushFile(file);
    delete file;
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::insertEntry(const void* key, const RecordId rid)
{
    if (key == nullptr)
        return;

    /* Get the root node */
    Page* currPage;
    bufMgr->readPage(file, rootPageNum, currPage);
    auto currNode = (NonLeafNodeInt*)currPage;

    LeafNodeInt* dataNode;
    int idx, intKey = *((int *)key);

    /* Store nodes in path to */
    std::stack<PageId> path;
    path.push(rootPageNum);

    /* iterate through the tree to find the right place for node insertion */
    while (true) {

        /* Iterate to get next pages */
        for (idx = 0;
             idx < INTARRAYNONLEAFSIZE && currNode->pageNoArray[idx + 1] != Page::INVALID_NUMBER && currNode->keyArray[idx] < intKey;
             idx++)
            ;

        /* check if index is new (new tree) */
        if (idx == 0 && currNode->pageNoArray[0] == Page::INVALID_NUMBER) {

            /* buffer allocate page */
            Page *pageRight, *pageLeft;
            PageId pageIdLeft, pageIdRight;
            bufMgr->allocPage(file, pageIdLeft, pageLeft);
            bufMgr->allocPage(file, pageIdRight, pageRight);

            /* set currnode as root */
            currNode->keyArray[0] = intKey;
            currNode->pageNoArray[0] = pageIdLeft;
            currNode->pageNoArray[1] = pageIdRight;

            /* init data */
            dataNode = (LeafNodeInt*)pageRight;
            auto leftDataNode = (LeafNodeInt*)pageLeft;
            leftDataNode->rightSibPageNo = pageIdRight;

            for (int i = 0; i < INTARRAYLEAFSIZE; ++i) {
                clearLeafNodeAtIdx(dataNode, i);
                clearLeafNodeAtIdx(leftDataNode, i);
            }

            /* Unpin page */
            try {
                bufMgr->unPinPage(file, pageIdLeft, true);
            }
            catch (PageNotPinnedException& e) {
            }

            path.push(pageIdRight);
            break;
        }

        /* Get next page in buffer*/
        bufMgr->readPage(file, currNode->pageNoArray[idx], currPage);
        path.push(currNode->pageNoArray[idx]);

        /* Set data node if its a leaf, otherwise cotinue iteration through the tree */
        if (currNode->level == 1) {
            dataNode = (LeafNodeInt*)currPage;
            break;
        }
        else {
            currNode = (NonLeafNodeInt*)currPage;
        }
    }

    /* check if it will split or insert directly */
    if (!insertKeyInLeafNode(dataNode, intKey, rid)) {

        /* Split the leaf node and copy the middle key up in the tree */
        PageId newPageId = splitLeafNode(dataNode, intKey, rid);

        try {
            bufMgr->unPinPage(file, path.top(), true);
        }
        catch (PageNotPinnedException& e) {
        }
        path.pop();

        PageId currPageId = path.top();

        /* Read the parent non-leaf node*/
        bufMgr->readPage(file, currPageId, currPage);
        try {
            bufMgr->unPinPage(file, currPageId, true);
        }
        catch (PageNotPinnedException& e) {
        }

        currNode = (NonLeafNodeInt*)currPage;

        /* Keep splitting until has space */
        while (!insertKeyInNonLeafNode(currNode, intKey, newPageId)) {

            newPageId = splitNonLeafNode(currNode, intKey, newPageId);

            try {
                bufMgr->unPinPage(file, currPageId, true);
            }
            catch (PageNotPinnedException& e) {
            }
            /* Unpin page and remove from path stack */
            path.pop();

            if (!path.empty()) {
                currPageId = path.top();
                bufMgr->readPage(file, currPageId, currPage);
                currNode = (NonLeafNodeInt*)currPage;
            }
            else {
                break;
            }
        }

        try {
            bufMgr->unPinPage(file, currPageId, true);
        }
        catch (PageNotPinnedException& e) {

        }

        /* No empty non-leaf node found, so create a new root */
        if (path.empty()) {
            Page* rootPage;
            PageId pageId;

            /* buffer allocate a new page for the root */
            bufMgr->allocPage(file, pageId, rootPage);

            /* Create the new root node */
            auto root = (NonLeafNodeInt*)rootPage;
            root->level = 0;

            for (int i = 1; i < INTARRAYNONLEAFSIZE; i++) {
                clearNonLeafNodeAtIdx(root, i);
            }
            root->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

            /* Copy the middle key and the page numbers of child nodes */
            root->keyArray[0] = intKey;
            root->pageNoArray[0] = currPageId;
            root->pageNoArray[1] = newPageId;

            /* Update the root page */
            rootPageNum = pageId;

            /* Unpin the new root page and the newly split child node */
            try {
                bufMgr->unPinPage(file, newPageId, true);
            }
            catch (PageNotPinnedException& e) {
            }
            try {
                bufMgr->unPinPage(file, pageId, true);
            }
            catch (PageNotPinnedException& e) {
            }
        }
        while (!path.empty()) {
            try {
                bufMgr->unPinPage(file, path.top(), true);
            }
            catch (PageNotPinnedException& e) {
            }
            path.pop();
        }
    }
    else {
        while (!path.empty()) {
            try {
                bufMgr->unPinPage(file, path.top(), true);
            }
            catch (PageNotPinnedException& e) {
            }
            catch (HashNotFoundException& e) {
            }
            path.pop();
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------
PageId BTreeIndex::splitLeafNode(LeafNodeInt* dataNode, int& intKey, const RecordId rid)
{
    /* Create and allocate leaf page */
    Page* page;
    PageId pageId;
    bufMgr->allocPage(file, pageId, page);
    auto newLeafNode = (LeafNodeInt*)page;

    /* Initialize the node with default values int */
    for (int i = 0; i < INTARRAYLEAFSIZE; i++)
        clearLeafNodeAtIdx(newLeafNode, i);

    /* get middle index */
    int midIdx = (INTARRAYLEAFSIZE + 1) / 2;

    /* Copy second half of data node to new leaf node and invalidate it in data node */
    for (int i = midIdx; i < INTARRAYLEAFSIZE; ++i) {
        newLeafNode->keyArray[i - midIdx] = dataNode->keyArray[i];
        newLeafNode->ridArray[i - midIdx] = dataNode->ridArray[i];
        clearLeafNodeAtIdx(dataNode, i);
    }

    if (intKey < newLeafNode->keyArray[0])
        insertKeyInLeafNode(dataNode, intKey, rid);
    else
        insertKeyInLeafNode(newLeafNode, intKey, rid);

    /* Update page IDs with right sib */
    newLeafNode->rightSibPageNo = dataNode->rightSibPageNo;
    dataNode->rightSibPageNo = pageId;

    intKey = newLeafNode->keyArray[0];

    /* Unpin the newly split child node */
    try {
        bufMgr->unPinPage(file, pageId, true);
    }
    catch (PageNotPinnedException& e) {
    }

    return pageId;
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------
PageId BTreeIndex::splitNonLeafNode(NonLeafNodeInt* node, int& intKey, const PageId pageId)
{
    /* Create and allocate the page */
    Page* page;
    PageId pageId_;
    bufMgr->allocPage(file, pageId_, page);
    auto newNode = (NonLeafNodeInt*)page;

    /* Initialize the node with default values */
    for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
        clearNonLeafNodeAtIdx(newNode, i);
    }
    newNode->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

    /* Get the middle index value */
    int midIdx = (INTARRAYNONLEAFSIZE + 1) / 2, prevKey = INT_MIN, i, j;
    int keyArr[INTARRAYNONLEAFSIZE + 1];
    PageId pageNoArr[INTARRAYNONLEAFSIZE + 2];

    /* first page remains the same as split occrus to the right side of onde */
    pageNoArr[0] = node->pageNoArray[0];

    /* Create a sorted array of all keys with new key in its position */
    for (i = 0, j = 0; j < INTARRAYNONLEAFSIZE; i++) {
        if (prevKey <= intKey && intKey < node->keyArray[j]) {
            keyArr[i] = intKey;
            pageNoArr[i + 1] = pageId;
            prevKey = node->keyArray[j];
            continue;
        }
        prevKey = keyArr[i] = node->keyArray[j];
        pageNoArr[i + 1] = node->pageNoArray[j + 1];
        j++;
    }
    /* Special case where the key is the last key in the sorted key list */
    if (i == j) {
        keyArr[i] = intKey;
        pageNoArr[i + 1] = pageId;
    }

    node->pageNoArray[0] = pageNoArr[0];

    /* Update keys of dataNode (left split) to the first half of keys */
    for (i = 0; i < midIdx; ++i) {
        node->keyArray[i] = keyArr[i];
        node->pageNoArray[i + 1] = pageNoArr[i + 1];
    }

    newNode->pageNoArray[0] = pageNoArr[midIdx + 1];
    /* Update keys of newNode (right split) with second half of keys */
    for (i = midIdx; i < INTARRAYNONLEAFSIZE; ++i) {
        newNode->keyArray[i - midIdx] = keyArr[i + 1];
        newNode->pageNoArray[i - midIdx + 1] = pageNoArr[i + 2];
        /* clear empty nodes in array */
        clearNonLeafNodeAtIdx(node, i);
        clearNonLeafNodeAtIdx(newNode, i - 1);
    }
    node->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

    newNode->level = node->level;

    intKey = keyArr[midIdx];

    try {
        bufMgr->unPinPage(file, pageId_, true);
    }
    catch (PageNotPinnedException& e) {
    }

    return pageId_;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertKeyInLeafNode
// -----------------------------------------------------------------------------
bool BTreeIndex::insertKeyInLeafNode(LeafNodeInt* node, int key, RecordId rid)
{
    /* Checks if the node contains any empty space for insertion */
    if (node->ridArray[INTARRAYLEAFSIZE - 1].page_number != Page::INVALID_NUMBER)
        return false;

    int idx, newKey = key;
    RecordId newRid = rid;

    /* Find the index to insert the key rid pair */
    for (idx = 0;
         idx < INTARRAYLEAFSIZE && node->ridArray[idx].page_number != Page::INVALID_NUMBER && node->keyArray[idx] < key;
         idx++)
        ;

    /* Insert the key at position idx and shift everything else right */
    for (; node->ridArray[idx].page_number != Page::INVALID_NUMBER; idx++) {
        int oldKey = node->keyArray[idx];
        RecordId oldRid = node->ridArray[idx];
        node->keyArray[idx] = newKey;
        node->ridArray[idx] = newRid;
        newKey = oldKey;
        newRid = oldRid;
    }
    node->keyArray[idx] = newKey;
    node->ridArray[idx] = newRid;

    return true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertKeyInNonLeafNode
// -----------------------------------------------------------------------------
bool BTreeIndex::insertKeyInNonLeafNode(NonLeafNodeInt* node, int key, PageId pageId)
{
    /* Checks if the node contains any empty space for insertion */
    if (node->pageNoArray[INTARRAYNONLEAFSIZE] != Page::INVALID_NUMBER)
        return false;

    int idx, newKey = key;
    PageId newPageId = pageId;

    /* Find the index to insert the key-pageId pair */
    for (idx = 0;
         idx < INTARRAYNONLEAFSIZE && node->pageNoArray[idx + 1] != Page::INVALID_NUMBER && node->keyArray[idx] < key;
         idx++)
        ;

    /* Insert the key at position idx and shift everything else right */
    for (; node->pageNoArray[idx + 1] != Page::INVALID_NUMBER; idx++) {
        int oldKey = node->keyArray[idx];
        PageId oldPageId = node->pageNoArray[idx + 1];
        node->keyArray[idx] = newKey;
        node->pageNoArray[idx + 1] = newPageId;
        newKey = oldKey;
        newPageId = oldPageId;
    }
    node->keyArray[idx] = newKey;
    node->pageNoArray[idx + 1] = newPageId;

    return true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::clearLeafNodeAtIdx
// -----------------------------------------------------------------------------
void BTreeIndex::clearLeafNodeAtIdx(LeafNodeInt* node, int idx)
{
    node->keyArray[idx] = -1;
    node->ridArray[idx].page_number = Page::INVALID_NUMBER;
    node->ridArray[idx].slot_number = Page::INVALID_SLOT;
}

// -----------------------------------------------------------------------------
// BTreeIndex::clearNonLeafNodeAtIdx
// -----------------------------------------------------------------------------
void BTreeIndex::clearNonLeafNodeAtIdx(NonLeafNodeInt* node, int idx)
{
    node->keyArray[idx] = -1;
    node->pageNoArray[idx] = Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
void BTreeIndex::startScan(const void* lowValParm,
    const Operator lowOpParm,
    const void* highValParm,
    const Operator highOpParm)
{
    /* Check aprameters values */
    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
        throw BadOpcodesException();
    }

    lowValInt = *(int*)lowValParm;
    highValInt = *(int*)highValParm;

    /* check bounds */
    if (lowValInt > highValInt)
        throw BadScanrangeException();

    if (scanExecuting) {
        endScan();
    }

    /* set scan */
    scanExecuting = true;
    lowOp = lowOpParm;
    highOp = highOpParm;

    /* Scan the tree from root to find the parent of the first leaf node to be scanned */
    getFirstParent(rootPageNum);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getFirstParent
// -----------------------------------------------------------------------------
void BTreeIndex::getFirstParent(PageId pageNum)
{
    currentPageNum = pageNum;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    auto nonLeafNode = (NonLeafNodeInt*)currentPageData;

    int i = 0;
    while (i < INTARRAYNONLEAFSIZE
        && lowValInt >= nonLeafNode->keyArray[i]
        && nonLeafNode->pageNoArray[i + 1] != Page::INVALID_NUMBER)
        i++;

    /* non leaf above leaf node */
    if (nonLeafNode->level == 1) {
        try {
            bufMgr->unPinPage(file, currentPageNum, false);
        }
        catch (PageNotPinnedException& e) {
        }

        /* Search for the key in leaf node */
        currentPageNum = nonLeafNode->pageNoArray[i];
        bufMgr->readPage(file, currentPageNum, currentPageData);

        /* binary search to set the value of nextEntry to read the first record that is in the scan range */
        auto currentNode = (LeafNodeInt*)currentPageData;
        int low = 0, high = INTARRAYLEAFSIZE - 1;
        int mid;
        while (low <= high) {
            mid = (low + high) / 2;

            if (currentNode->ridArray[mid].page_number == Page::INVALID_NUMBER) {
                high = mid - 1;
            }
            else if ((lowOp == GT && currentNode->keyArray[mid] == lowValInt + 1) || (lowOp == GTE && currentNode->keyArray[mid] == lowValInt)) {
                break;
            }
            else if ((lowOp == GT && currentNode->keyArray[mid] <= lowValInt) || (lowOp == GTE && currentNode->keyArray[mid] < lowValInt)) {
                low = mid + 1;
            }
            else {
                high = mid - 1;
            }
        }
        nextEntry = mid;
    }
    else {
        /* unpin page and move on to the next page, no recrod */
        try {
            bufMgr->unPinPage(file, currentPageNum, false);
        }
        catch (PageNotPinnedException& e) {
        }
        getFirstParent(nonLeafNode->pageNoArray[i]);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
void BTreeIndex::scanNext(RecordId& outRid)
{
    /* Check if scan has started */
    if (!scanExecuting)
        throw ScanNotInitializedException();

    /* Keep track of node */
    auto currentNode = (LeafNodeInt*)currentPageData;

    /* Look for rid of next matching tuple */
    while (true) {
        /* Validate index of entry */
        if (nextEntry == INTARRAYLEAFSIZE) {
            /* Unpin page since no more entries to be scanned on this leaf page */
            try {
                bufMgr->unPinPage(file, currentPageNum, false);
            }
            catch (PageNotPinnedException& e) {
            }

            PageId rightSibPageNo = currentNode->rightSibPageNo;

            /* Check that the right sibling is a valid leaf page */
            if (rightSibPageNo == Page::INVALID_NUMBER)
                /* trhow new completed */
                throw IndexScanCompletedException();

            /* Update the parameters for the index since page is invalid */
            nextEntry = 0;
            currentPageNum = rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            currentNode = (LeafNodeInt*)currentPageData;
        }

        if (currentNode->ridArray[nextEntry].page_number == Page::INVALID_NUMBER) {
            nextEntry = INTARRAYLEAFSIZE;
            continue;
        }

        /* Check lower limit of scan with entry key */
        if ((lowOp == GT && currentNode->keyArray[nextEntry] <= lowValInt) || (lowOp == GTE && currentNode->keyArray[nextEntry] < lowValInt)) {
            nextEntry++;
            continue;
        }

        /* Check upper limit of scan with entry key */
        if ((highOp == LT && currentNode->keyArray[nextEntry] >= highValInt)
            || (highOp == LTE && currentNode->keyArray[nextEntry] > highValInt))
            throw IndexScanCompletedException();

        break;
    }

    /* Return  entry rid */
    outRid = currentNode->ridArray[nextEntry];

    /* update index of next entry */
    nextEntry++;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan()
{
    /* check scan */
    if (!scanExecuting)
        throw ScanNotInitializedException();

    /* End scan */
    scanExecuting = false;

    /* Unpin the pages that are currently pinned */
    try {
        bufMgr->unPinPage(file, currentPageNum, false);
    }
    catch (PageNotPinnedException& e) {
    }
    catch (HashNotFoundException& e) {
    }
}
}

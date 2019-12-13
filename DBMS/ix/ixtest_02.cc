#include "ix.h"
#include "ix_test_util.h"

int testCase_2(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Disk I/O check of Insertion - CollectCounterValues **
    // 4. print B+ Tree **
    // 5. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 02 *****" << std::endl;

    RID rid;
    int key = 200;
    rid.pageNum = 500;
    rid.slotNum = 20;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixFileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << std::endl << "Before Insert - R W A: " << readPageCount << " " << writePageCount << " "
              << appendPageCount
              << std::endl;

    // insert entry
    rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // collect counters
    rc = ixFileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After Insert - R W A: " << readPageCountAfter << " " << writePageCountAfter << " "
              << appendPageCountAfter
              << std::endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    std::cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff
              << std::endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        std::cerr << "Insertion should generate some page I/O. The implementation is not correct." << std::endl;
        rc = indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    std::cerr << std::endl;
    indexManager.printBtree(ixFileHandle, attribute);

    // close index file
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

int main() {

    const std::string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    if (testCase_2(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 02 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 02 failed. *****" << std::endl;
        return fail;
    }

}


#include "ix.h"
#include "ix_test_util.h"

int testCase_4(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Open Index file
    // 2. Disk I/O check of deleteEntry - CollectCounterValues **
    // 3. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 04 *****" << std::endl;

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

    std::cerr << "Before DeleteEntry - R W A: " << readPageCount << " " << writePageCount << " " << appendPageCount
              << std::endl;

    // delete entry
    rc = indexManager.deleteEntry(ixFileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::deleteEntry() should not fail.");

    rc = ixFileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After DeleteEntry - R W A: " << readPageCountAfter << " " << writePageCountAfter << " "
              << appendPageCountAfter << std::endl;

    // collect counters
    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    std::cerr << "Page I/O count of single deletion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff
              << std::endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        std::cerr << "Deletion should generate some page I/O. The implementation is not correct." << std::endl;
        rc = indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // delete entry again - should fail
    rc = indexManager.deleteEntry(ixFileHandle, attribute, &key, rid);
    assert(rc != success && "indexManager::deleteEntry() should fail.");

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

    if (testCase_4(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 04 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 04 failed. *****" << std::endl;
        return fail;
    }

}


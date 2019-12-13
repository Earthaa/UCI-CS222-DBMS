#include "ix.h"
#include "ix_test_util.h"

int testCase_3(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Open Index file
    // 2. Disk I/O check of Scan and getNextEntry - CollectCounterValues **
    // 3. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 03 *****" << std::endl;

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

    IX_ScanIterator ix_ScanIterator;

    // open index file
    IXFileHandle ixFileHandle;
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixFileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "Before scan - R W A: " << readPageCount << " " << writePageCount << " " << appendPageCount
              << std::endl;

    // Conduct a scan
    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // There should be one record
    int count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cerr << "Returned rid from a scan: " << rid.pageNum << " " << rid.slotNum << std::endl;
        assert(rid.pageNum == 500 && "rid.pageNum is not correct.");
        assert(rid.slotNum == 20 && "rid.slotNum is not correct.");
        count++;
    }
    assert(count == 1 && "scan count is not correct.");

    // collect counters
    rc = ixFileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After scan - R W A: " << readPageCountAfter << " " << writePageCountAfter << " "
              << appendPageCountAfter << std::endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    std::cerr << "Page I/O count of scan - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << std::endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        std::cerr << "Scan should generate some page I/O. The implementation is not correct." << std::endl;
        ix_ScanIterator.close();
        indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // Close index file
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

    if (testCase_3(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 03 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 03 failed. *****" << std::endl;
        return fail;
    }

}


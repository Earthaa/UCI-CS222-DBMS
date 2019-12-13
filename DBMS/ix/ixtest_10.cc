#include "ix.h"
#include "ix_test_util.h"

int testCase_10(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entries with two different keys
    // 4. Scan entries that match one of the keys **
    // 5. Scan close **
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 10 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key1 = 200;
    unsigned key2 = 500;
    unsigned numOfTuples = 50;

    int inRidSlotNumSum = 0;
    int outRidSlotNumSum = 0;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    for (unsigned i = 0; i <= numOfTuples / 2; i++) {
        rid.pageNum = i + 1;
        rid.slotNum = i + 2;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key1, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
        inRidSlotNumSum += rid.slotNum;

        rid.pageNum = i + 101;
        rid.slotNum = i + 102;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key2, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // Scan
    rc = indexManager.scan(ixFileHandle, attribute, &key1, &key1, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // iterate
    unsigned count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key1) == success) {
        count++;
        if (count % 10 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
        outRidSlotNumSum += rid.slotNum;
    }

    // Inconsistency?
    if (inRidSlotNumSum != outRidSlotNumSum) {
        std::cerr << "Wrong entries output... The test failed" << std::endl;
        rc = ix_ScanIterator.close();
        rc = indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // Close Index
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

    indexManager.destroyFile("age_idx");

    if (testCase_10(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 10 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 10 failed. *****" << std::endl;
        return fail;
    }
}

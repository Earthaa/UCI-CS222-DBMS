#include "ix.h"
#include "ix_test_util.h"

int testCase_6(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open
    // 5. Scan close **
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 06 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRidSlotNumSum = 0;
    int outRidSlotNumSum = 0;
    unsigned numOfTuples = 1000;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    for (unsigned i = 0; i <= numOfTuples; i++) {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key * 3;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        inRidSlotNumSum += rid.slotNum;
    }

    // Scan
    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // Fetch all entries
    int count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        count++;

        if (rid.pageNum % 200 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
        outRidSlotNumSum += rid.slotNum;
    }

    // Inconsistency between insert and scan?
    if (inRidSlotNumSum != outRidSlotNumSum) {
        std::cerr << "Wrong entries output... The test failed." << std::endl;
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

    if (testCase_6(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 06 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 06 failed. *****" << std::endl;
        return fail;
    }

}


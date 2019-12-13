#include "ix.h"
#include "ix_test_util.h"

int testCase_7(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Open Index File that created by test case 6
    // 2. Scan entries NO_OP -- open
    // 3. Scan close
    // 4. Close Index File
    // 5. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 07 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRidSlotNumSum = 0;
    int outRidSlotNumSum = 0;
    unsigned numOfTuples = 1000;

    // open index file
    RC rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // compute inRidPageNumSum without inserting entries
    for (unsigned i = 0; i <= numOfTuples; i++) {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key * 3;

        inRidSlotNumSum += rid.slotNum;
    }

    // scan
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

    // scan fail?
    if (inRidSlotNumSum != outRidSlotNumSum) {
        std::cerr << "Wrong entries output... The test failed." << std::endl;
        rc = ix_ScanIterator.close();
        rc = indexManager.closeFile(ixFileHandle);
        rc = indexManager.destroyFile(indexFileName);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // Close Index
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    // Destroy Index
    rc = indexManager.destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;

}

int main() {

    const std::string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    if (testCase_7(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 07 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 07 failed. *****" << std::endl;
        return fail;
    }

}


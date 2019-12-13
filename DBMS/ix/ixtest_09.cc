#include "ix.h"
#include "ix_test_util.h"

int testCase_9(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries using LT_OP operator and checking if the values returned are correct.
    //    Returned values are part of two separate insertions. **
    // 5. Scan close
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 09 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 2000;
    unsigned numOfMoreTuples = 6000;
    float key;
    float compVal = 6500;
    int inRidSlotNumSum = 0;
    int outRidSlotNumSum = 0;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    for (unsigned i = 1; i <= numOfTuples; i++) {
        key = (float) i + 87.6;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        if (key < compVal) {
            inRidSlotNumSum += rid.slotNum;
        }
    }

    // insert more entries
    for (unsigned i = 6000; i <= numOfTuples + numOfMoreTuples; i++) {
        key = (float) i + 87.6;
        rid.pageNum = i;
        rid.slotNum = i - (unsigned) 500;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        if (key < compVal) {
            inRidSlotNumSum += rid.slotNum;
        }
    }

    // scan
    rc = indexManager.scan(ixFileHandle, attribute, NULL, &compVal, true, false, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // iterate
    unsigned count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        count++;
        if (rid.pageNum % 500 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
        outRidSlotNumSum += rid.slotNum;
    }

    // Inconsistency between input and output?
    if (inRidSlotNumSum != outRidSlotNumSum) {
        std::cerr << "Wrong entries output... The test failed" << std::endl;
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

    const std::string indexFileName = "height_idx";
    Attribute attrHeight;
    attrHeight.length = 4;
    attrHeight.name = "height";
    attrHeight.type = TypeReal;

    indexManager.destroyFile("height_idx");

    if (testCase_9(indexFileName, attrHeight) == success) {
        std::cerr << "***** IX Test Case 09 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 09 failed. *****" << std::endl;
        return fail;
    }

}


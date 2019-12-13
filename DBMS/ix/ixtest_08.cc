#include "ix.h"
#include "ix_test_util.h"

int testCase_8(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries using GE_OP operator and checking if the values returned are correct. **
    // 5. Scan close
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 08 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 300;
    unsigned numOfMoreTuples = 100;
    unsigned key;
    int inRidSlotNumSum = 0;
    int outRidSlotNumSum = 0;
    unsigned value = 7001;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert Entries
    for (unsigned i = 1; i <= numOfTuples; i++) {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key * 3;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // Insert more entries
    for (unsigned i = value; i < value + numOfMoreTuples; i++) {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key * 3;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        inRidSlotNumSum += rid.slotNum;
    }

    // Scan
    rc = indexManager.scan(ixFileHandle, attribute, &value, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // IndexScan iterator
    unsigned count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        count++;

        if (rid.pageNum % 100 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
        if (rid.pageNum < value || rid.slotNum < value * 3) {
            std::cerr << "Wrong entries output... The test failed" << std::endl;
            rc = ix_ScanIterator.close();
            rc = indexManager.closeFile(ixFileHandle);
            rc = indexManager.destroyFile(indexFileName);
            return fail;
        }
        outRidSlotNumSum += rid.slotNum;
    }

    // Inconsistency check
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

    indexManager.destroyFile("age_idx");

    if (testCase_8(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 08 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 08 failed. *****" << std::endl;
        return fail;
    }

}


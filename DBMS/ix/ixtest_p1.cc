#include "ix.h"
#include "ix_test_util.h"

RC closeWithFail(const string &indexFileName1, const string &indexFileName2, IXFileHandle &ixFileHandle1,
                 IXFileHandle &ixFileHandle2, IX_ScanIterator &ix_ScanIterator1, IX_ScanIterator &ix_ScanIterator2) {
    ix_ScanIterator1.close();
    ix_ScanIterator2.close();

    indexManager.closeFile(ixFileHandle1);
    indexManager.closeFile(ixFileHandle2);

    indexManager.destroyFile(indexFileName1);
    indexManager.destroyFile(indexFileName2);

    return fail;
}

int testCase_p1(const std::string &indexFileName1, const std::string &indexFileName2, const Attribute &attribute) {
    // Check whether multiple indexes can be used at the same time.
    std::cerr << std::endl << "***** In IX Test Private Case 1 *****" << std::endl;

    RID rid;
    RID rid2;
    IXFileHandle ixFileHandle1;
    IXFileHandle ixFileHandle2;
    IX_ScanIterator ix_ScanIterator1;
    IX_ScanIterator ix_ScanIterator2;
    unsigned numOfTuples = 2000;
    float key;
    float key2;
    float compVal = 6500;
    unsigned inRidPageNumSum = 0;
    unsigned outRidPageNumSum = 0;

    // create index files
    RC rc = indexManager.createFile(indexFileName1);
    assert(rc == success && "indexManager::createFile() should not fail.");

    rc = indexManager.createFile(indexFileName2);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open the index files
    rc = indexManager.openFile(indexFileName1, ixFileHandle1);
    assert(rc == success && "indexManager::openFile() should not fail.");

    rc = indexManager.openFile(indexFileName2, ixFileHandle2);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entry
    for (unsigned i = 1; i <= numOfTuples; i++) {
        key = (float) i + 87.6;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager.insertEntry(ixFileHandle1, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        rc = indexManager.insertEntry(ixFileHandle2, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        if (key < compVal) {
            inRidPageNumSum += rid.pageNum;
        }
    }

    // insert more entries
    for (unsigned i = 6000; i <= numOfTuples + 6000; i++) {
        key = (float) i + 87.6;
        rid.pageNum = i;
        rid.slotNum = i - (unsigned) 500;

        // insert entry
        rc = indexManager.insertEntry(ixFileHandle1, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        // insert entry
        rc = indexManager.insertEntry(ixFileHandle2, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        if (key < compVal) {
            inRidPageNumSum += rid.pageNum;
        }
    }

    // Conduct a scan
    rc = indexManager.scan(ixFileHandle1, attribute, NULL, &compVal, true, false, ix_ScanIterator1);
    assert(rc == success && "indexManager::scan() should not fail.");

    // Conduct a scan
    rc = indexManager.scan(ixFileHandle2, attribute, NULL, &compVal, true, false, ix_ScanIterator2);
    assert(rc == success && "indexManager::scan() should not fail.");

    int returnedCount = 0;
    while (ix_ScanIterator1.getNextEntry(rid, &key) == success) {
        returnedCount++;

        if (ix_ScanIterator2.getNextEntry(rid2, &key2) != success) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                                 ix_ScanIterator2);
        }
        if (rid.pageNum != rid2.pageNum) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                                 ix_ScanIterator2);
        }
        if (rid.pageNum % 1000 == 0) {
            std::cerr << returnedCount << " - returned entries: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
        outRidPageNumSum += rid.pageNum;
    }

    if (inRidPageNumSum != outRidPageNumSum) {
        std::cerr << "Wrong entries output...failure" << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                             ix_ScanIterator2);

    }

    // Close Scan
    rc = ix_ScanIterator1.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    rc = ix_ScanIterator2.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");


    // Close index file
    rc = indexManager.closeFile(ixFileHandle1);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    rc = indexManager.closeFile(ixFileHandle2);
    assert(rc == success && "indexManager::closeFile() should not fail.");


    // Destroy Index
    rc = indexManager.destroyFile(indexFileName1);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    rc = indexManager.destroyFile(indexFileName2);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;
}

int main() {

    const std::string indexHeightFileName1 = "private_height_idx1";
    const std::string indexHeightFileName2 = "private_height_idx2";
    Attribute attrHeight;
    attrHeight.length = 4;
    attrHeight.name = "Height";
    attrHeight.type = TypeReal;

    indexManager.destroyFile(indexHeightFileName1);
    indexManager.destroyFile(indexHeightFileName2);

    if (testCase_p1(indexHeightFileName1, indexHeightFileName2, attrHeight) == success) {
        std::cerr << "***** IX Test Private Case 1 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 1 failed. *****" << std::endl;
        return fail;
    }

}

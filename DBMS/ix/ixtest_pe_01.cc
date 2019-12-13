#include "ix.h"
#include "ix_test_util.h"

int testCase_extra_1(const std::string &indexFileName, const Attribute &attribute) {
    // Checks whether duplicated entries spanning multiple page are handled properly or not.
    std::cerr << std::endl << "***** In IX Private Test Extra Case 01 *****" << std::endl;

    RID rid;
    unsigned numOfTuples = 10000;
    unsigned numExtra = 5000;
    unsigned key;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    int compVal1 = 9, compVal2 = 15;
    int count = 0;

    //create index file(s)
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    //open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entry
    for (unsigned i = 1; i <= numOfTuples; i++) {
        key = i % 10;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    for (unsigned i = numOfTuples; i < numOfTuples + numExtra; i++) {
        key = i % 10 + 10;
        rid.pageNum = i;
        rid.slotNum = i + 10;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // scan
    rc = indexManager.scan(ixFileHandle, attribute, &compVal1, &compVal1, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // iterate
    count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        count++;

        if (rid.pageNum != rid.slotNum || key != compVal1) {
            std::cerr << "Wrong entries output... The test failed" << std::endl;
        }

        if (count % 100 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
    }

    std::cerr << "Number of scanned entries: " << count << std::endl << std::endl;
    if (count != 1000) {
        std::cerr << "Wrong entries output... The test failed" << std::endl;
        ix_ScanIterator.close();
        indexManager.closeFile(ixFileHandle);
        indexManager.destroyFile(indexFileName);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // scan
    rc = indexManager.scan(ixFileHandle, attribute, &compVal2, &compVal2, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        count++;

        if (rid.pageNum != (rid.slotNum - 10) || key != compVal2) {
            std::cerr << "Wrong entries output... The test failed" << std::endl;
        }

        if (count % 100 == 0) {
            std::cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
    }

    std::cerr << "Number of scanned entries: " << count << std::endl;
    if (count != 500) {
        std::cerr << "Wrong entries output... The test failed" << std::endl;
        ix_ScanIterator.close();
        indexManager.closeFile(ixFileHandle);
        indexManager.destroyFile(indexFileName);
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

    const std::string indexFileName = "private_extra_age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    indexManager.destroyFile(indexFileName);

    if (testCase_extra_1(indexFileName, attrAge) == success) {
        std::cerr << "IX_Test Private Extra Case 01 finished. The result will be examined." << std::endl;
        return success;
    } else {
        std::cerr << "IX_Test Private Extra Case 01 failed." << std::endl;
        return fail;
    }

}

#include <algorithm>
#include <random>
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

int testCase_p2(const std::string &indexFileName1, const std::string &indexFileName2, const Attribute &attribute) {

    // insert 30,000 entries to two indexes
    // scan and delete
    // insert 20,000 entries to two indexes
    // scan

    std::cerr << std::endl << "***** In IX Test Private Case 2 *****" << std::endl;

    RID rid;
    RID rid2;
    IXFileHandle ixFileHandle1;
    IXFileHandle ixFileHandle2;
    IX_ScanIterator ix_ScanIterator1;
    IX_ScanIterator ix_ScanIterator2;
    int compVal;
    int numOfTuples;
    int A[20000];
    int B[30000];
    int count = 0;
    int key;
    int key2;

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


    // Prepare key entries
    numOfTuples = 20000;
    for (int i = 0; i < numOfTuples; i++) {
        A[i] = i;
    }

    // Randomly shuffle the entries
    std::shuffle(A, A + numOfTuples, std::mt19937(std::random_device()()));

    // Insert entries
    for (int i = 0; i < numOfTuples; i++) {
        key = A[i];
        rid.pageNum = i + 1;
        rid.slotNum = i + 1;

        rc = indexManager.insertEntry(ixFileHandle1, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        rc = indexManager.insertEntry(ixFileHandle2, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    compVal = 5000;

    // Conduct a scan
    rc = indexManager.scan(ixFileHandle1, attribute, NULL, &compVal, true, true, ix_ScanIterator1);
    assert(rc == success && "indexManager::scan() should not fail.");

    rc = indexManager.scan(ixFileHandle2, attribute, NULL, &compVal, true, true, ix_ScanIterator2);
    assert(rc == success && "indexManager::scan() should not fail.");

    // scan & delete
    count = 0;
    while (ix_ScanIterator1.getNextEntry(rid, &key) == success) {
        if (ix_ScanIterator2.getNextEntry(rid2, &key2) != success
            || rid.pageNum != rid2.pageNum) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                                 ix_ScanIterator2);
        }

        // delete entry
        rc = indexManager.deleteEntry(ixFileHandle1, attribute, &key, rid);
        assert(rc == success && "indexManager::deleteEntry() should not fail.");

        rc = indexManager.deleteEntry(ixFileHandle2, attribute, &key, rid);
        assert(rc == success && "indexManager::deleteEntry() should not fail.");

        count++;
    }
    if (count != 5001) {
        std::cerr << count << " - Wrong entries output...failure" << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                             ix_ScanIterator2);

    }

    // close scan
    rc = ix_ScanIterator1.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    rc = ix_ScanIterator2.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");


    // insert more entries Again
    numOfTuples = 30000;
    for (int i = 0; i < numOfTuples; i++) {
        B[i] = 20000 + i;
    }
    std::random_shuffle(B, B + numOfTuples);

    for (int i = 0; i < numOfTuples; i++) {
        key = B[i];
        rid.pageNum = i + 20001;
        rid.slotNum = i + 20001;

        rc = indexManager.insertEntry(ixFileHandle1, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        rc = indexManager.insertEntry(ixFileHandle2, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // scan
    compVal = 35000;

    rc = indexManager.scan(ixFileHandle1, attribute, NULL, &compVal, true, true, ix_ScanIterator1);
    assert(rc == success && "indexManager::scan() should not fail.");

    rc = indexManager.scan(ixFileHandle2, attribute, NULL, &compVal, true, true, ix_ScanIterator2);
    assert(rc == success && "indexManager::scan() should not fail.");

    count = 0;
    while (ix_ScanIterator1.getNextEntry(rid, &key) == success) {
        if (ix_ScanIterator2.getNextEntry(rid2, &key) != success) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                                 ix_ScanIterator2);
        }
        if (rid.pageNum > 20000 && B[rid.pageNum - 20001] > 35000) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                                 ix_ScanIterator2);
        }
        count++;
    }
    if (count != 30000) {
        std::cerr << count << " - Wrong entries output...failure" << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2, ix_ScanIterator1,
                             ix_ScanIterator2);
    }

    //close scan
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

    const std::string indexAgeFileName1 = "private_age_idx1";
    const std::string indexAgeFileName2 = "private_age_idx2";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "Age";
    attrAge.type = TypeInt;

    indexManager.destroyFile(indexAgeFileName1);
    indexManager.destroyFile(indexAgeFileName2);

    if (testCase_p2(indexAgeFileName1, indexAgeFileName2, attrAge) == success) {
        std::cerr << "***** IX Test Private Case 2 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 2 failed. *****" << std::endl;
        return fail;
    }

}
 

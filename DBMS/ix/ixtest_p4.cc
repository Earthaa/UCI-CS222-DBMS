
#include "ix.h"
#include "ix_test_util.h"

RC closeWithFail(const string &indexFileName1, const string &indexFileName2, IXFileHandle &ixFileHandle1,
                 IXFileHandle &ixFileHandle2) {
    indexManager.closeFile(ixFileHandle1);
    indexManager.closeFile(ixFileHandle2);
    indexManager.destroyFile(indexFileName1);
    indexManager.destroyFile(indexFileName2);

    return fail;
}

int testCase_p4(const std::string &indexFileName1, const Attribute &attribute1, const std::string &indexFileName2,
                const Attribute &attribute2) {
    // Checks whether varchar key is handled properly.
    std::cerr << std::endl << "***** In IX Test Private Case 4 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle1;
    IX_ScanIterator ix_ScanIterator1;
    IXFileHandle ixFileHandle2;
    IX_ScanIterator ix_ScanIterator2;

    unsigned readPage1 = 0;
    unsigned writePage1 = 0;
    unsigned appendPage1 = 0;
    unsigned readPage2 = 0;
    unsigned writePage2 = 0;
    unsigned appendPage2 = 0;

    char key[100];
    int numOfTuples = 50000;
    int i = 0;
    *(int *) key = 5;
    int count = 0;

    char lowKey[100];
    char highKey[100];

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


    // insert entries
    for (i = 1; i <= numOfTuples; i++) {
        sprintf(key + 4, "%05d", i);
        rid.pageNum = i;
        rid.slotNum = i % PAGE_SIZE;

        rc = indexManager.insertEntry(ixFileHandle1, attribute1, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        rc = indexManager.insertEntry(ixFileHandle2, attribute2, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // collect counters
    rc = ixFileHandle1.collectCounterValues(readPage1, writePage1, appendPage1);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    rc = ixFileHandle2.collectCounterValues(readPage2, writePage2, appendPage2);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    if (writePage1 < 1) {
        std::cerr << "Did not use disk at all. Test failed." << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2);

    }

    // Actually, there should be no difference.
    if (writePage2 + appendPage2 - writePage1 - appendPage1 > 10) {
        std::cerr << "Failed to handle space nicely for VarChar keys..." << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2);
    }

    *(int *) lowKey = 5;
    sprintf(lowKey + 4, "%05d", 30801);
    *(int *) highKey = 5;
    sprintf(highKey + 4, "%05d", 30900);

    rc = indexManager.scan(ixFileHandle1, attribute1, lowKey, highKey, true, true, ix_ScanIterator1);
    assert(rc == success && "indexManager::scan() should not fail.");

    rc = indexManager.scan(ixFileHandle2, attribute2, lowKey, highKey, true, true, ix_ScanIterator2);
    assert(rc == success && "indexManager::scan() should not fail.");

    //iterate
    count = 0;
    while (ix_ScanIterator1.getNextEntry(rid, &key) != IX_EOF) {
        if (ix_ScanIterator2.getNextEntry(rid, &key) != success) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2);
        }
        count++;
    }
    if (count != 100) {
        std::cerr << "Wrong output count! expected: 100, actual: " << count << " ...Failure" << std::endl;
        return closeWithFail(indexFileName1, indexFileName2, ixFileHandle1, ixFileHandle2);
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

    const std::string indexEmpNameFileName1 = "private_empname_shortidx";
    const std::string indexEmpNameFileName2 = "private_empname_longidx";
    Attribute attrShortEmpName;
    attrShortEmpName.length = 10;
    attrShortEmpName.name = "ShortEmpName";
    attrShortEmpName.type = TypeVarChar;
    Attribute attrLongEmpName;
    attrLongEmpName.length = 100;
    attrLongEmpName.name = "LongEmpName";
    attrLongEmpName.type = TypeVarChar;

    indexManager.destroyFile(indexEmpNameFileName1);
    indexManager.destroyFile(indexEmpNameFileName2);

    if (testCase_p4(indexEmpNameFileName1, attrShortEmpName, indexEmpNameFileName2, attrLongEmpName) == success) {
        std::cerr << "***** IX Test Private Case 4 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 4 failed. *****" << std::endl;
        return fail;
    }
}

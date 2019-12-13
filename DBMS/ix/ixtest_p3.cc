#include "ix.h"
#include "ix_test_util.h"

int testCase_p3(const std::string &indexFileName, const Attribute &attribute) {
    std::cerr << std::endl << "***** In IX Test Private Case 3 *****" << std::endl;

    // Varchar index handling check
    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    char key[100];
    int numOfTuples = 100000;
    int i = 0;
    *(int *) key = 6;
    int count = 0;
    char lowKey[100];
    char highKey[100];

    // create index files
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open the index files
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entry
    for (i = 1; i <= numOfTuples; i++) {
        sprintf(key + 4, "%06d", i);
        rid.pageNum = i;
        rid.slotNum = i % PAGE_SIZE;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    *(int *) lowKey = 6;
    sprintf(lowKey + 4, "%06d", 90000);
    *(int *) highKey = 6;
    sprintf(highKey + 4, "%06d", 100000);

    // Conduct a scan
    rc = indexManager.scan(ixFileHandle, attribute, lowKey, highKey, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    //iterate
    count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
        key[10] = '\0';
        if (count % 2000 == 0) {
            fprintf(stderr, "output: %s\n", key + 4);
        }
        count++;
    }

    if (count != 10001) {
        std::cerr << "Wrong output count! expected: 10001" << ", actual: " << count << " Failure" << std::endl;
        ix_ScanIterator.close();
        indexManager.closeFile(ixFileHandle);
        indexManager.destroyFile(indexFileName);
        return fail;
    }

    // close scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // Close index file
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    // Destroy Index
    rc = indexManager.destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;

}

int main() {

    const std::string indexEmpNameFileName1 = "private_empname_shortidx";
    Attribute attrShortEmpName;
    attrShortEmpName.length = 20;
    attrShortEmpName.name = "ShortEmpName";
    attrShortEmpName.type = TypeVarChar;

    indexManager.destroyFile(indexEmpNameFileName1);

    if (testCase_p3(indexEmpNameFileName1, attrShortEmpName) == success) {
        std::cerr << "***** IX Test Private Case 3 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 3 failed. *****" << std::endl;
        return fail;
    }

}



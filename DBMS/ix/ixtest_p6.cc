#include "ix.h"
#include "ix_test_util.h"

int testCase_p6(const std::string &indexFileName, const Attribute &attribute) {
    // Checks whether duplicated entries in a page are handled properly.

    std::cerr << std::endl << "***** In IX Test Private Case 6 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 90;
    char key[100];
    *(int *) key = 5;
    int count = 0;

    char lowKey[100];
    char highKey[100];

    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    // insert entries
    for (unsigned i = 0; i < numOfTuples; i++) {
        sprintf(key + 4, "%05d", i % 3);

        rid.pageNum = i;
        rid.slotNum = i % 3;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        if (i % 3 == 1) {
            inRidPageNumSum += rid.pageNum;
        }
    }

    // eyeball check: a key only appears once in each node (both inner nodes and leaf nodes)
    // Actually, this should print out only one page.
    indexManager.printBtree(ixFileHandle, attribute);

    *(int *) lowKey = 5;
    sprintf(lowKey + 4, "%05d", 1);
    *(int *) highKey = 5;
    sprintf(highKey + 4, "%05d", 1);

    // scan
    rc = indexManager.scan(ixFileHandle, attribute, lowKey, highKey, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    //iterate
    count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
        if (rid.slotNum != 1) {
            std::cerr << "Wrong entries output...failure" << std::endl;
            ix_ScanIterator.close();
            indexManager.closeFile(ixFileHandle);
            indexManager.destroyFile(indexFileName);
            return fail;
        }
        outRidPageNumSum += rid.pageNum;
        count++;
    }
    std::cerr << "The number of scanned entries: " << count << std::endl;
    if (count != 30 || outRidPageNumSum != inRidPageNumSum || inRidPageNumSum == 0 || outRidPageNumSum == 0) {
        std::cerr << "Wrong entries output...failure " << std::endl;
        indexManager.closeFile(ixFileHandle);
        indexManager.destroyFile(indexFileName);
        return fail;
    }

    // Close Index
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    // Destroy Index
    rc = indexManager.destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;
}

int main() {

    const std::string indexEmpNameFileName = "private_empname_shortidx";
    Attribute attrShortEmpName;
    attrShortEmpName.length = 10;
    attrShortEmpName.name = "ShortEmpName";
    attrShortEmpName.type = TypeVarChar;

    indexManager.destroyFile(indexEmpNameFileName);

    if (testCase_p6(indexEmpNameFileName, attrShortEmpName) == success) {
        std::cerr << "***** IX Test Private Case 6 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 6 failed. *****" << std::endl;
        return fail;
    }

}


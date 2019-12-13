#include "ix.h"
#include "ix_test_util.h"

int testCase_15(const std::string &indexFileName, const Attribute &attribute) {
    // Checks whether duplicated entries in a page are handled properly.
    //
    // Functions tested
    // 1. OpenIndex
    // 2. Insert entries with the same key
    // 3. Print BTree
    // 4. CloseIndex
    // 5. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.

    std::cerr << std::endl << "***** In IX Test Case 15 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    int key = 300;
    unsigned numOfTuples = 50;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    for (unsigned i = 0; i <= numOfTuples; i++) {
        rid.pageNum = numOfTuples + i + 1;
        rid.slotNum = i + 2;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // eyeball check: a key only appears once in each node (both inner nodes and leaf nodes)
    // Actually, this should print out only one page.
    indexManager.printBtree(ixFileHandle, attribute);

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

    if (testCase_15(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 15 finished. Please check the shape of the B+ Tree. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 15 failed. *****" << std::endl;
        return fail;
    }

}


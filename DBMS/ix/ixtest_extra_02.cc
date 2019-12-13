#include "ix.h"
#include "ix_test_util.h"

void prepareKeyAndRid(const unsigned count, const unsigned i, char *key, RID &rid) {
    *(unsigned *) key = count;
    for (unsigned j = 0; j < count; j++) {
        key[4 + j] = 'a' + i - 1;
    }
    rid.pageNum = i;
    rid.slotNum = i;
}

int testCase_extra_2(const std::string &indexFileName,
                     const Attribute &attribute) {
    // Checks whether the deletion is properly managed (non-lazy deletion)
    // Functions tested
    // 1. CreateIndex
    // 2. OpenIndex
    // 3. Insert entries to make a 3 level tree 
    // 4. Print BTree 
    // 5. Delete the "unsafe one" 
    // 6. CloseIndex
    // 7. DestroyIndex
    std::cerr << std::endl << "***** In IX Test Extra Case 01 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 13;
    char key[PAGE_SIZE];
    unsigned count = attribute.length;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    unsigned i = 1;
    for (; i <= numOfTuples; i++) {
        prepareKeyAndRid(count, i, key, rid);

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // print BTree, by this time the BTree should have 3 level
    indexManager.printBtree(ixFileHandle, attribute);

    // delete the 2nd entry
    prepareKeyAndRid(count, 2, key, rid);
    rc = indexManager.deleteEntry(ixFileHandle, attribute, key, rid);
    assert(rc == success && "indexManager::deleteEntry() should not fail.");

    std::cerr << std::endl << std::endl << "/////////////////" << std::endl << std::endl;

    // print BTree, by this time the BTree should have 2 level
    indexManager.printBtree(ixFileHandle, attribute);
    std::cerr << std::endl;

    // Close Index
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    // Destroy Index
    rc = indexManager.destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;

}

int main() {

    const std::string indexEmpNameFileName = "EmpName_idx";

    Attribute attrEmpName;
    attrEmpName.length = PAGE_SIZE / 5;  // Each node could only have 4 children
    attrEmpName.name = "EmpName";
    attrEmpName.type = TypeVarChar;

    indexManager.destroyFile("EmpName_idx");

    if (testCase_extra_2(indexEmpNameFileName, attrEmpName) == success) {
        std::cerr << "IX_Test Case Extra 02 finished. Please check the shape of B+ Tree"
                     " to make sure non lazy-deletion is applied." << std::endl;
        return success;
    } else {
        std::cerr << "IX_Test Case Extra 02 failed." << std::endl;
        return fail;
    }

}




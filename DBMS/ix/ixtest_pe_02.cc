#include "ix.h"
#include "ix_test_util.h"

void prepareKeyAndRid(const unsigned count, const unsigned i, char *key, RID &rid) {
    *(int *) key = count;
    for (unsigned j = 0; j < count; j++) {
        *(key + 4 + j) = 96 + i;
    }
    rid.pageNum = i;
    rid.slotNum = i;
}

int testCase_Private_Extra_2(const std::string &indexFileName,
                             const Attribute &attribute) {
    // Checks whether the deletion is properly managed (non-lazy deletion)
    std::cerr << std::endl << "***** In IX Private Extra Test Case 02 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 6;
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

    // print BTree, by this time the BTree should have 2 level
    //    [d]         [c]
    // [abc def] or [ab cdef]
    indexManager.printBtree(ixFileHandle, attribute);

    // delete the fifth and sixth entry
    prepareKeyAndRid(count, 5, key, rid);
    rc = indexManager.deleteEntry(ixFileHandle, attribute, key, rid);
    assert(rc == success && "indexManager::deleteEntry() should not fail.");

    // After deleting two entries (e,f):
    // [abcd]
    //
    // If lazy-deletion is applied:
    //   [d]          [c]
    // [abc d]   or [ab cd]
    prepareKeyAndRid(count, 6, key, rid);
    rc = indexManager.deleteEntry(ixFileHandle, attribute, key, rid);
    assert(rc == success && "indexManager::deleteEntry() should not fail.");

    std::cerr << std::endl << std::endl << "/////////////////" << std::endl << std::endl;
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

    const std::string indexEmpNameFileName = "private_empname_idx";

    Attribute attrEmpName;
    attrEmpName.length = PAGE_SIZE / 5;  // Each node could only have 4 children
    attrEmpName.name = "EmpName";
    attrEmpName.type = TypeVarChar;

    indexManager.destroyFile(indexEmpNameFileName);

    if (testCase_Private_Extra_2(indexEmpNameFileName, attrEmpName) == success) {
        std::cerr << "IX_Test Private Extra Case 02 finished. The result will be examined." << std::endl;
        return success;
    } else {
        std::cerr << "IX_Test Private Extra Case 02 failed." << std::endl;
        return fail;
    }

}





#include "ix.h"
#include "ix_test_util.h"

void prepareKeyAndRid(const unsigned count, const unsigned i, char *key, RID &rid) {
    *(int *) key = count;
    for (unsigned j = 0; j < count; j++) {
        key[4 + j] = 'a' + i - 1;
    }
    rid.pageNum = i;
    rid.slotNum = i;
}

int testCase_p5(const std::string &indexFileName, const Attribute &attribute) {

    // Checks whether leaves are linked and the way of conducting search is correct.
    std::cerr << std::endl << "***** In IX Test Private Case 5 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 7;
    char key[PAGE_SIZE];
    unsigned count = attribute.length;

    // create index files
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entry
    unsigned i = 1;
    for (; i <= numOfTuples; i++) {
        prepareKeyAndRid(count, i * 10, key, rid);

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // insert the 8th
    prepareKeyAndRid(count, i++ * 10, key, rid);
    rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // print BTree, by this time the BTree should have 2 or 3 level 
    // depend on the design of your root
    indexManager.printBtree(ixFileHandle, attribute);

    // insert the 9th
    prepareKeyAndRid(count, i++ * 10, key, rid);
    rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    unsigned readPageCountInsert = 0;
    unsigned writePageCountInsert = 0;
    unsigned appendPageCountInsert = 0;

    unsigned readPageCountScan = 0;
    unsigned writePageCountScan = 0;
    unsigned appendPageCountScan = 0;

    // collect counters
    rc = ixFileHandle.collectCounterValues(readPageCountInsert, writePageCountInsert, appendPageCountInsert);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After Insertion - R:" << readPageCountInsert << " W:" << writePageCountInsert << " A:"
              << appendPageCountInsert << std::endl;

    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    rc = ixFileHandle.collectCounterValues(readPageCountScan, writePageCountScan, appendPageCountScan);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After Initialization of Scan - R:" << readPageCountScan << " W:" << writePageCountScan << " A:"
              << appendPageCountScan << std::endl;

    count = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        std::cerr << "Returned rid:" << rid.pageNum << "," << rid.slotNum << std::endl;
        count++;
    }

    rc = ixFileHandle.collectCounterValues(readPageCountScan, writePageCountScan, appendPageCountScan);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    std::cerr << "After Iteration - R:" << readPageCountScan << " W:" << writePageCountScan << " A:"
              << appendPageCountScan << std::endl;

    unsigned roughLeafReadCount = readPageCountScan - readPageCountInsert;
    // If the B+Tree index is 3 level: 3 I/O + 9 scan I/O per entry at maximum  = 12 
    // If the B+Tree index is 2 level: 2 I/O + 9 scan I/O per entry at maximum  = 11 
    if (roughLeafReadCount > 12) {
        std::cerr << "Too many read I/Os for scan: " << roughLeafReadCount << ", the leaf nodes should be linked."
                  << std::endl;
        std::cerr << "Check the print out B+ Tree to validate the pages" << std::endl;
        indexManager.printBtree(ixFileHandle, attribute);
        indexManager.closeFile(ixFileHandle);
        indexManager.destroyFile(indexFileName);

        return fail;
    }


    // Close index file
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
    attrEmpName.length = PAGE_SIZE / 4;  // each node could only have 3 children
    attrEmpName.name = "EmpName";
    attrEmpName.type = TypeVarChar;

    indexManager.destroyFile(indexEmpNameFileName);

    if (testCase_p5(indexEmpNameFileName, attrEmpName) == success) {
        std::cerr << "***** IX Test Private Case 5 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Private Case 5 failed. *****" << std::endl;
        return fail;
    }

}

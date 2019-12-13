#include "ix.h"
#include "ix_test_util.h"

RC testCase_1(const std::string &indexFileName) {
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Create Index File -- when index file is already created **
    // 4. Open Index File ** -- when a file handle is already opened **
    // 5. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cerr << std::endl << "***** In IX Test Case 01 *****" << std::endl;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixFileHandle;
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // create duplicate index file
    rc = indexManager.createFile(indexFileName);
    assert(rc != success && "Calling indexManager::createFile() on an existing file should fail.");

    // open index file again using the file handle that is already opened.
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc != success && "Calling indexManager::openFile() using an already opened file handle should fail.");

    // close index file
    rc = indexManager.closeFile(ixFileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

int main() {

    const std::string indexFileName = "age_idx";
    indexManager.destroyFile("age_idx");

    if (testCase_1(indexFileName) == success) {
        std::cerr << "***** IX Test Case 01 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 01 failed. *****" << std::endl;
        return fail;
    }
}


#include "ix.h"
#include "ix_test_util.h"

int testCase_5(const std::string &indexFileName, const Attribute &attribute) {
    // Functions tested
    // 1. Destroy Index File **
    // 2. Open Index File -- should fail
    // 3. Scan  -- should fail
    std::cerr << std::endl << "***** In IX Test Case 05 *****" << std::endl;

    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;

    // destroy index file
    RC rc = indexManager.destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    // Try to open the destroyed index
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc != success && "indexManager::openFile() on a non-existing file should fail.");

    // Try to conduct a scan on the destroyed index
    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc != success && "indexManager::scan() on a non-existing file should fail.");

    return success;

}

int main() {

    const std::string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    if (testCase_5(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 05 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 05 failed. *****" << std::endl;
        return fail;
    }

}

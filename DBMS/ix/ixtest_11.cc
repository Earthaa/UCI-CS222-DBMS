
#include "ix.h"
#include "ix_test_util.h"

int testCase_11(const std::string &indexFileName, const Attribute &attribute) {
    // Create Index file
    // Open Index file
    // Insert large number of records
    // Scan large number of records to validate insert correctly
    // Delete some tuples
    // Insert large number of records again
    // Scan large number of records to validate insert correctly
    // Delete all
    // Close Index
    // Destroy Index

    std::cerr << std::endl << "***** In IX Test Case 11 *****" << std::endl;

    RID rid;
    IXFileHandle ixFileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    unsigned inRecordNum = 0;
    unsigned outRecordNum = 0;
    unsigned numOfTuples = 1000 * 1000;

    // create index file
    RC rc = indexManager.createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert entries
    for (unsigned i = 0; i <= numOfTuples; i++) {
        key = i;
        rid.pageNum = key + 1;
        rid.slotNum = key + 2;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
        inRecordNum += 1;
        if (inRecordNum % 200000 == 0) {
            std::cerr << inRecordNum << " inserted - rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
    }

    // scan
    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // Iterate
    std::cerr << std::endl;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        if (rid.pageNum != key + 1 || rid.slotNum != key + 2) {
            std::cerr << "Wrong entries output... The test failed." << std::endl;
            rc = ix_ScanIterator.close();
            rc = indexManager.closeFile(ixFileHandle);
            return fail;
        }
        outRecordNum += 1;
        if (outRecordNum % 200000 == 0) {
            std::cerr << outRecordNum << " scanned. " << std::endl;
        }
    }

    // Inconsistency?
    if (inRecordNum != outRecordNum || inRecordNum == 0 || outRecordNum == 0) {
        std::cerr << "Wrong entries output... The test failed." << std::endl;
        rc = ix_ScanIterator.close();
        rc = indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // Delete some tuples
    std::cerr << std::endl;
    unsigned deletedRecordNum = 0;
    for (unsigned i = 5; i <= numOfTuples; i += 10) {
        key = i;
        rid.pageNum = key + 1;
        rid.slotNum = key + 2;

        rc = indexManager.deleteEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::deleteEntry() should not fail.");

        deletedRecordNum += 1;
        if (deletedRecordNum % 20000 == 0) {
            std::cerr << deletedRecordNum << " deleted. " << std::endl;
        }
    }

    // Close Scan and reinitialize the scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "IX_ScanIterator::scan() should not fail.");

    std::cerr << std::endl;
    // Iterate
    outRecordNum = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        if (rid.pageNum != key + 1 || rid.slotNum != key + 2) {
            std::cerr << "Wrong entries output... The test failed." << std::endl;
            rc = ix_ScanIterator.close();
            rc = indexManager.closeFile(ixFileHandle);
            return fail;
        }
        outRecordNum += 1;
        if (outRecordNum % 200000 == 0) {
            std::cerr << outRecordNum << " scanned. " << std::endl;
        }

    }
    std::cerr << outRecordNum << " scanned. " << std::endl;

    // Inconsistency?
    if ((inRecordNum - deletedRecordNum) != outRecordNum || inRecordNum == 0 || deletedRecordNum == 0 ||
        outRecordNum == 0) {
        std::cerr << "Wrong entries output... The test failed." << std::endl;
        rc = ix_ScanIterator.close();
        rc = indexManager.closeFile(ixFileHandle);
        return fail;
    }

    // Insert the deleted entries again
    int reInsertedRecordNum = 0;
    std::cerr << std::endl;
    for (unsigned i = 5; i <= numOfTuples; i += 10) {
        key = i;
        rid.pageNum = key + 1;
        rid.slotNum = key + 2;

        rc = indexManager.insertEntry(ixFileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        reInsertedRecordNum += 1;
        if (reInsertedRecordNum % 20000 == 0) {
            std::cerr << reInsertedRecordNum << " inserted - rid: " << rid.pageNum << " " << rid.slotNum << std::endl;
        }
    }

    // Close Scan and reinitialize the scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    rc = indexManager.scan(ixFileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "IX_ScanIterator::scan() should not fail.");

    // Iterate
    std::cerr << std::endl;
    outRecordNum = 0;
    while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
        if (rid.pageNum != key + 1 || rid.slotNum != key + 2) {
            std::cerr << "Wrong entries output... The test failed." << std::endl;
            rc = ix_ScanIterator.close();
            rc = indexManager.closeFile(ixFileHandle);
            return fail;
        }
        outRecordNum += 1;

        if (outRecordNum % 200000 == 0) {
            std::cerr << outRecordNum << " scanned. " << std::endl;
        }

    }

    // Inconsistency?
    if ((inRecordNum - deletedRecordNum + reInsertedRecordNum) != outRecordNum || inRecordNum == 0
        || reInsertedRecordNum == 0 || outRecordNum == 0) {
        std::cerr << "Wrong entries output... The test failed." << std::endl;
        rc = ix_ScanIterator.close();
        rc = indexManager.closeFile(ixFileHandle);
        rc = indexManager.destroyFile(indexFileName);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

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

    if (testCase_11(indexFileName, attrAge) == success) {
        std::cerr << "***** IX Test Case 11 finished. The result will be examined. *****" << std::endl;
        return success;
    } else {
        std::cerr << "***** [FAIL] IX Test Case 11 failed. *****" << std::endl;
        return fail;
    }
}


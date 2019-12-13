#include "rm_test_util.h"

RC TEST_RM_11(const std::string &tableName, std::vector<RID> &rids) {
    // Functions Tested for large tables:
    // 1. delete tuple
    // 2. read tuple
    std::cout << std::endl << "***** In RM Test Case 11 *****" << std::endl;

    int numTuples = 2000;
    RC rc = 0;
    void *returnedData = malloc(4000);

    readRIDsFromDisk(rids, numTuples);

    // Delete the first 1000 tuples
    for (int i = 0; i < 1000; i++) {
        rc = rm.deleteTuple(tableName, rids[i]);
        assert(rc == success && "RelationManager::deleteTuple() should not fail.");
    }

    // Try to read the first 1000 deleted tuples
    for (int i = 0; i < 1000; i++) {
        rc = rm.readTuple(tableName, rids[i], returnedData);
        assert(rc != success && "RelationManager::readTuple() on a deleted tuple should fail.");
    }

    for (int i = 1000; i < 2000; i++) {
        rc = rm.readTuple(tableName, rids[i], returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");
    }
    std::cout << "***** Test Case 11 Finished. The result will be examined. *****" << std::endl << std::endl;

    free(returnedData);

    return success;
}

int main() {
    std::vector<RID> rids;
    std::vector<int> sizes;

    // Delete Tuple
    return TEST_RM_11("tbl_employee4", rids);
}

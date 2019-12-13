#include "rm_test_util.h"

RC TEST_RM_7(const string &tableName) {
    // Functions Tested
    // 1. Simple scan **
    // 2. Delete the given table
    std::cout << std::endl << "***** In RM Test Case 7 *****" << std::endl;

    RID rid;
    int numTuples = 100;
    void *returnedData = malloc(200);

    std::set<int> ages;
    RC rc = 0;
    for (int i = 0; i < numTuples; i++) {
        int age = 20 + i;
        ages.insert(age);
    }

    // Set up the iterator
    RM_ScanIterator rmsi;
    std::string attr = "Age";
    std::vector<std::string> attributes;
    attributes.push_back(attr);
    rc = rm.scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");
    int ageReturned = 0;

    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        //std::cout << "Returned Age: " << *(int *)((char *)returnedData+1) <<std::endl;
        ageReturned = *(int *) ((char *) returnedData + 1);
        if (ages.find(ageReturned) == ages.end()) {
            std::cout << "***** [FAIL] Test Case 7 Failed *****" << std::endl << std::endl;
            rmsi.close();
            free(returnedData);
            return -1;
        }
    }
    rmsi.close();

    // Delete a Table
    rc = rm.deleteTable(tableName);
    assert(rc == success && "RelationManager::deleteTable() should not fail.");

    free(returnedData);
    std::cout << "***** Test Case 7 Finished. The result will be examined. *****" << std::endl << std::endl;
    return success;
}

int main() {
    // Simple Scan
    return TEST_RM_7("tbl_employee3");
}

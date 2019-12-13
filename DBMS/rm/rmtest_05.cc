#include "rm_test_util.h"

RC TEST_RM_5(const string &tableName, const int nameLength, const string &name, const int age, const float height,
             const int salary) {
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Table **
    // 3. Read Tuple
    // 4. Insert Tuple
    std::cout << std::endl << "***** In RM Test Case 5 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);
    void *returnedData1 = malloc(200);

    // Test Insert Tuple
    vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Test Read Tuple 
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    // Test Delete Table
    rc = rm.deleteTable(tableName);
    assert(rc == success && "RelationManager::deleteTable() should not fail.");

    // Reading a tuple on a deleted table
    memset((char *) returnedData1, 0, 200);
    rc = rm.readTuple(tableName, rid, returnedData1);
    assert(rc != success && "RelationManager::readTuple() on a deleted table should fail.");

    // Inserting a tuple on a deleted table
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc != success && "RelationManager::insertTuple() on a deleted table should fail.");

    if (memcmp(returnedData, returnedData1, tupleSize) != 0) {
        std::cout << "***** Test Case 5 Finished. The result will be examined. *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(returnedData1);
        free(nullsIndicator);
        return success;
    } else {
        std::cout << "***** [FAIL] Test Case 5 Failed *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(returnedData1);
        free(nullsIndicator);
        return -1;
    }
}

int main() {
    // Delete Table
    return TEST_RM_5("tbl_employee", 6, "Martin", 29, 193.6, 20000);
}

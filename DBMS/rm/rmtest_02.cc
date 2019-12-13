#include "rm_test_util.h"

RC TEST_RM_2(const std::string &tableName, const int nameLength, const std::string &name, const int age,
             const float height, const int salary) {
    // Functions Tested
    // 1. Insert tuple
    // 2. Delete Tuple **
    // 3. Read Tuple
    std::cout << std::endl << "***** In RM Test Case 2 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    // Test Insert the Tuple
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    std::cout << "The tuple to be inserted:" << std::endl;
    rm.printTuple(attrs, tuple);
    std::cout << std::endl;
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Delete the tuple
    rc = rm.deleteTuple(tableName, rid);
    assert(rc == success && "RelationManager::deleteTuple() should not fail.");

    // Read Tuple after deleting it - should fail
    memset(returnedData, 0, 200);
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc != success && "Reading a deleted tuple should fail.");

    // Compare the two memory blocks to see whether they are different
    if (memcmp(tuple, returnedData, tupleSize) != 0) {
        std::cout << "***** RM Test Case 2 finished. The result will be examined. *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return success;
    } else {
        std::cout << "***** [FAIL] RM Test case 2 failed *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

}

int main() {
    // Delete Tuple
    return TEST_RM_2("tbl_employee", 5, "Peter", 23, 5.11, 12000);
}

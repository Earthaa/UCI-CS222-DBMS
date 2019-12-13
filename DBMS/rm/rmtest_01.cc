#include "rm_test_util.h"

RC TEST_RM_1(const std::string &tableName, const int nameLength, const std::string &name, const int age,
             const float height, const int salary) {
    // Functions tested
    // 1. Insert Tuple **
    // 2. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case. 
    std::cout << std::endl << "***** In RM Test Case 1 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    // Initialize a NULL field indicator
    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Insert a tuple into a table
    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    std::cout << "The tuple to be inserted:" << std::endl;
    rm.printTuple(attrs, tuple);
    std::cout << std::endl;

    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Given the rid, read the tuple from table
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    std::cout << "The returned tuple:" << std::endl;
    rm.printTuple(attrs, returnedData);
    std::cout << std::endl;

    // Compare whether the two memory blocks are the same
    if (memcmp(tuple, returnedData, tupleSize) == 0) {
        std::cout << "**** RM Test Case 1 finished. The result will be examined. *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return success;
    } else {
        std::cout << "**** [FAIL] RM Test Case 1 failed *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

}

int main() {
    // Insert/Read Tuple
    return TEST_RM_1("tbl_employee", 14, "Peter Anteater", 27, 6.2, 10000);
}

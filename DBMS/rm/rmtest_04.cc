#include "rm_test_util.h"

RC TEST_RM_4(const std::string &tableName, const int nameLength, const std::string &name, const int age,
             const float height, const int salary) {
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes **
    std::cout << std::endl << "***** In RM Test Case 4 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    // Test Insert the Tuple
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Test Read Attribute
    rc = rm.readAttribute(tableName, rid, "Salary", returnedData);
    assert(rc == success && "RelationManager::readAttribute() should not fail.");

    int salaryBack = *(int *) ((char *) returnedData + nullAttributesIndicatorActualSize);

    std::cout << "Salary: " << salary << " Returned Salary: " << salaryBack << std::endl;
    if (memcmp((char *) returnedData + nullAttributesIndicatorActualSize,
               (char *) tuple + 19 + nullAttributesIndicatorActualSize, 4) == 0) {
        std::cout << "***** RM Test case 4 Finished. The result will be examined. *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return success;
    } else {
        std::cout << "***** [FAIL] RM Test Case 4 Failed. *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

}

int main() {
    // Read Attributes
    return TEST_RM_4("tbl_employee", 7, "Hoffman", 31, 5.8, 9999);
}

#include "rm_test_util.h"

RC RM_TEST_EXTRA_2(const std::string &tableName, const int nameLength, const std::string &name, const int age,
                   const int height, const int salary, const int ssn) {
    // Functions Tested
    // 1. Add Attribute **
    // 2. Insert Tuple
    std::cout << std::endl << "***** In RM Extra Credit Test Case 2 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    // Test Add Attribute
    Attribute attr;
    attr.name = "SSN";
    attr.type = TypeInt;
    attr.length = 4;
    RC rc = rm.addAttribute(tableName, attr);
    assert(rc == success && "RelationManager::addAttribute() should not fail.");

    // GetAttributes
    std::vector<Attribute> attrs;
    rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Test Insert Tuple
    prepareTupleAfterAdd(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, ssn, tuple, &tupleSize);
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Test Read Tuple
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    std::cout << "Inserted Data:" << std::endl;
    rc = rm.printTuple(attrs, tuple);

    std::cout << std::endl << "Returned Data:" << std::endl;
    rc = rm.printTuple(attrs, returnedData);

    if (memcmp(returnedData, tuple, tupleSize) != 0) {
        std::cout << "***** [FAIL] Extra Credit Test Case 2 Failed *****" << std::endl << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    } else {
        std::cout << "***** Extra Credit Test Case 2 Finished. The result will be examined. *****" << std::endl
                  << std::endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return success;
    }

}

int main() {
    std::string name2 = "Victors";

    // Drop table for the case where we execute this test multiple times.
    // We ignore the error code for this operation.
    rm.deleteTable("tbl_employee200");

    createTable("tbl_employee200");

    // Add Attributes
    return RM_TEST_EXTRA_2("tbl_employee200", 7, name2, 22, 180, 6000, 123479765);

}

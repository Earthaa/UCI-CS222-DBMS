#include "rm_test_util.h"

RC RM_TEST_EXTRA_1(const std::string &tableName, const int nameLength, const std::string &name, const int age,
                   const int height, const int salary) {
    // Extra Test Case - Functions Tested:
    // 1. Insert tuple
    // 2. Read Attributes
    // 3. Drop Attributes **
    std::cout << std::endl << "***** In RM Extra Credit Test Case 1 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    // Insert Tuple
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Read Attribute
    rc = rm.readAttribute(tableName, rid, "Height", returnedData);
    assert(rc == success && "RelationManager::readAttribute() should not fail.");

    if (memcmp((char *) returnedData + nullAttributesIndicatorActualSize,
               (char *) tuple + 22 + nullAttributesIndicatorActualSize, 4) != 0) {
        std::cout << "RelationManager::readAttribute() failed." << std::endl;
        std::cout << "***** [FAIL] Extra Credit Test Case 1 Failed. *****" << std::endl;
        free(returnedData);
        free(tuple);
        free(nullsIndicator);
        return -1;
    } else {
        // Drop the attribute
        rc = rm.dropAttribute(tableName, "Height");
        assert(rc == success && "RelationManager::dropAttribute() should not fail.");

        // Read Tuple and print the tuple
        rc = rm.readTuple(tableName, rid, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");

        // Get the attribute from the table again
        std::vector<Attribute> attrs2;
        rc = rm.getAttributes(tableName, attrs2);

        // The size of the original attribute vector size should be greater than the current one.
        if (attrs.size() <= attrs2.size()) {
            std::cout << "***** [FAIL] Extra Credit Test Case 1 Failed. *****" << std::endl;
            free(tuple);
            free(returnedData);
            free(nullsIndicator);
        } else {
            rc = rm.printTuple(attrs2, returnedData);
            assert(rc == success && "RelationManager::printTuple() should not fail.");
        }
    }

    free(tuple);
    free(returnedData);
    free(nullsIndicator);

    std::cout << "***** Extra Credit Test Case 1 finished. The result will be examined. *****" << std::endl;
    return success;
}

int main() {
    std::string name1 = "Peter Anteater";

    // Drop table for the case where we execute this test multiple times.
    // We ignore the error code for this operation.
    rm.deleteTable("tbl_employee100");

    // Create a table
    createTable("tbl_employee100");
    return RM_TEST_EXTRA_1("tbl_employee100", 14, name1, 24, 185, 10000);
}

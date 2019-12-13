#include "rm_test_util.h"

RC TEST_RM_3(const std::string &tableName, const int nameLength, const std::string &name, const int age,
             const float height, const int salary) {
    // Functions Tested
    // 1. Insert Tuple    
    // 2. Update Tuple **
    // 3. Read Tuple
    std::cout << std::endl << "***** In RM Test Case 3****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    unsigned updatedTupleSize = 0;
    void *tuple = malloc(200);
    void *updatedTuple = malloc(200);
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
    std::cout << "Original RID:  " << rid.pageNum << " " << rid.slotNum << std::endl;

    // Test Update Tuple
    prepareTuple(attrs.size(), nullsIndicator, 7, "Barbara", age, height, 12000, updatedTuple, &updatedTupleSize);
    rc = rm.updateTuple(tableName, updatedTuple, rid);
    assert(rc == success && "RelationManager::updateTuple() should not fail.");

    // Test Read Tuple 
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    // Print the tuples 
    std::cout << "Inserted Data:" << std::endl;
    rm.printTuple(attrs, tuple);
    std::cout << std::endl;

    std::cout << "Updated data:" << std::endl;
    rm.printTuple(attrs, updatedTuple);
    std::cout << std::endl;

    std::cout << "Returned Data:" << std::endl;
    rm.printTuple(attrs, returnedData);
    std::cout << std::endl;

    if (memcmp(updatedTuple, returnedData, updatedTupleSize) == 0) {
        std::cout << "***** RM Test Case 3 Finished. The result will be examined. *****" << std::endl << std::endl;
        free(tuple);
        free(updatedTuple);
        free(returnedData);
        free(nullsIndicator);
        return 0;
    } else {
        std::cout << "***** [FAIL] RM Test case 3 Failed *****" << std::endl << std::endl;
        free(tuple);
        free(updatedTuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

}

int main() {
    // Update Tuple
    return TEST_RM_3("tbl_employee", 4, "Paul", 28, 6.5, 6000);
}

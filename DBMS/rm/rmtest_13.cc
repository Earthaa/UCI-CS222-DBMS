#include "rm_test_util.h"

RC TEST_RM_13(const std::string &tableName) {
    // Functions Tested:
    // 1. Conditional scan
    std::cout << std::endl << "***** In RM Test Case 13 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    int numTuples = 500;
    void *tuple;
    void *returnedData = malloc(200);
    int ageVal = 25;
    int age = 0;

    RID rids[numTuples];
    std::vector<char *> tuples;

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    for (int i = 0; i < numTuples; i++) {
        tuple = malloc(100);
        // Insert Tuple
        auto height = (float) i;

        age = (rand() % 10) + 23;

        prepareTuple(attrs.size(), nullsIndicator, 6, "Tester", age, height, 123, tuple, &tupleSize);
        rc = rm.insertTuple(tableName, tuple, rid);
        assert(rc == success && "RelationManager::insertTuple() should not fail.");

        rids[i] = rid;
        free(tuple);
    }
    // Set up the iterator
    RM_ScanIterator rmsi;
    std::string attr = "Age";
    std::vector<std::string> attributes;
    attributes.push_back(attr);
    rc = rm.scan(tableName, attr, GT_OP, &ageVal, attributes, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        age = *(int *) ((char *) returnedData + 1);
        if (age <= ageVal) {
            std::cout << "Returned value from a scan is not correct." << std::endl;
            std::cout << "***** [FAIL] Test Case 13 Failed *****" << std::endl << std::endl;
            rmsi.close();
            free(returnedData);
            free(nullsIndicator);
            return -1;
        }
    }
    rmsi.close();
    free(returnedData);
    free(nullsIndicator);

    rc = rm.deleteTable("tbl_b_employee4");

    std::cout << "***** Test Case 13 Finished. The result will be examined. *****" << std::endl << std::endl;

    return success;
}

int main() {
    // Scan with conditions
    createTable("tbl_b_employee4");
    return TEST_RM_13("tbl_b_employee4");
}

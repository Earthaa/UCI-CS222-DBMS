#include "rm_test_util.h"

RC TEST_RM_6(const std::string &tableName) {
    // Functions Tested
    // 1. Simple scan **
    std::cout << std::endl << "***** In RM Test Case 6 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    int numTuples = 100;
    void *tuple;
    void *returnedData = malloc(200);

    // Test Insert Tuple
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    RID rids[numTuples];
    std::set<int> ages;
    for (int i = 0; i < numTuples; i++) {
        tuple = malloc(200);

        // Insert Tuple
        float height = (float) i;
        int age = 20 + i;
        prepareTuple(attrs.size(), nullsIndicator, 6, "Tester", age, height, age * 10, tuple, &tupleSize);
        ages.insert(age);
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
    rc = rm.scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributes.size());
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        if (ages.find(*(int *) ((char *) returnedData + nullAttributesIndicatorActualSize)) == ages.end()) {
            std::cout << "***** [FAIL] Test Case 6 Failed *****" << std::endl << std::endl;
            rmsi.close();
            free(returnedData);
            free(nullsIndicator);
            return -1;
        }
    }
    rmsi.close();

    free(returnedData);
    free(nullsIndicator);
    std::cout << "***** Test Case 6 Finished. The result will be examined. *****" << std::endl << std::endl;
    return 0;
}

int main() {
    // Simple Scan
    return TEST_RM_6("tbl_employee3");
}

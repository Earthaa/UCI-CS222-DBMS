#include "rm_test_util.h"

RC TEST_RM_13b(const std::string &tableName) {
    // Functions Tested:
    // 1. Conditional scan - including NULL values
    std::cout << std::endl << "***** In RM Test Case 13B *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    int numTuples = 500;
    void *tuple;
    void *returnedData = malloc(200);
    int ageVal = 25;
    int age = 0;

    RID rids[numTuples];
    std::vector<char *> tuples;
    std::string tupleName;
    char *suffix = (char *) malloc(10);

    bool nullBit = false;

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    auto *nullsIndicatorWithNull = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicatorWithNull, 0, nullAttributesIndicatorActualSize);

    // age field : NULL
    nullsIndicatorWithNull[0] = 64; // 01000000

    for (int i = 0; i < numTuples; i++) {
        tuple = malloc(100);

        // Insert Tuple
        auto height = (float) i;

        age = (rand() % 20) + 15;

        sprintf(suffix, "%d", i);

        if (i % 10 == 0) {
            tupleName = "TesterNull";
            tupleName += suffix;
            prepareTuple(attrs.size(), nullsIndicatorWithNull, tupleName.length(), tupleName, 0, height, 456, tuple,
                         &tupleSize);
        } else {
            tupleName = "Tester";
            tupleName += suffix;
            prepareTuple(attrs.size(), nullsIndicator, tupleName.length(), tupleName, age, height, 123, tuple,
                         &tupleSize);
        }
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
        // Check the first bit of the returned data since we only return one attribute in this test case
        // However, the age with NULL should not be returned since the condition NULL > 25 can't hold.
        // All comparison operations with NULL should return FALSE
        // (e.g., NULL > 25, NULL >= 25, NULL <= 25, NULL < 25, NULL == 25, NULL != 25: ALL FALSE)
        nullBit = *(unsigned char *) ((char *) returnedData) & ((unsigned) 1 << (unsigned) 7);
        if (!nullBit) {
            age = *(int *) ((char *) returnedData + 1);
            if (age <= ageVal) {
                // Comparison didn't work in this case
                std::cout << "Returned value from a scan is not correct: returned Age <= 25." << std::endl;
                std::cout << "***** [FAIL] Test Case 13B Failed *****" << std::endl << std::endl;
                rmsi.close();
                free(returnedData);
                free(suffix);
                free(nullsIndicator);
                free(nullsIndicatorWithNull);
                return -1;
            }
        } else {
            // Age with NULL value should not be returned.
            std::cout << "Returned value from a scan is not correct. NULL returned." << std::endl;
            std::cout << "***** [FAIL] Test Case 13B Failed *****" << std::endl << std::endl;
            rmsi.close();
            free(returnedData);
            free(suffix);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);
            return -1;
        }
    }
    rmsi.close();
    free(returnedData);
    free(suffix);
    free(nullsIndicator);
    free(nullsIndicatorWithNull);

    rc = rm.deleteTable("tbl_b_employee5");

    std::cout << "Test Case 13B Finished. The result will be examined. *****" << std::endl << std::endl;

    return success;
}

int main() {
    // Scan with conditions
    createTable("tbl_b_employee5");
    return TEST_RM_13b("tbl_b_employee5");
}

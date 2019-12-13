#include "rm_test_util.h"

RC TEST_RM_09(const std::string &tableName, std::vector<RID> &rids, std::vector<int> &sizes) {
    // Functions Tested for large tables:
    // 1. read tuple
    std::cout << "***** In RM Test case 9 *****" << std::endl;

    int size = 0;
    int numTuples = 2000;
    void *tuple = malloc(4000);
    void *returnedData = malloc(4000);

    // read the saved rids and the sizes of records
    readRIDsFromDisk(rids, numTuples);
    readSizesFromDisk(sizes, numTuples);

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    for (int i = 0; i < numTuples; i++) {
        memset(tuple, 0, 4000);
        memset(returnedData, 0, 4000);
        rc = rm.readTuple(tableName, rids[i], returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");

        size = 0;
        prepareLargeTuple(attrs.size(), nullsIndicator, i, tuple, &size);
        if (memcmp(returnedData, tuple, sizes[i]) != 0) {
            std::cout << "***** [FAIL] Test Case 9 Failed *****" << std::endl << std::endl;
            free(tuple);
            free(returnedData);
            free(nullsIndicator);
            return -1;
        }
    }

    free(tuple);
    free(returnedData);
    free(nullsIndicator);

    std::cout << "***** Test Case 9 Finished. The result will be examined. *****" << std::endl << std::endl;

    return success;
}

int main() {
    std::vector<RID> rids;
    std::vector<int> sizes;

    // Read Tuple
    return TEST_RM_09("tbl_employee4", rids, sizes);
}

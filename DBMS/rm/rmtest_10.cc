#include "rm_test_util.h"

RC TEST_RM_10(const std::string &tableName, std::vector<RID> &rids, std::vector<int> &sizes) {
    // Functions Tested for large tables:
    // 1. update tuple
    // 2. read tuple
    std::cout << std::endl << "***** In RM Test case 10 *****" << std::endl;

    int numTuples = 2000;
    void *tuple = malloc(4000);
    void *returnedData = malloc(4000);

    readRIDsFromDisk(rids, numTuples);
    readSizesFromDisk(sizes, numTuples);

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Update the first 1000 tuples
    int size = 0;
    for (int i = 0; i < 1000; i++) {
        memset(tuple, 0, 4000);
        RID rid = rids[i];

        prepareLargeTuple(attrs.size(), nullsIndicator, i + 10, tuple, &size);
        rc = rm.updateTuple(tableName, tuple, rid);
        assert(rc == success && "RelationManager::updateTuple() should not fail.");

        sizes[i] = size;
        rids[i] = rid;
    }

    // Read the updated records and check the integrity
    for (int i = 0; i < 1000; i++) {
        memset(tuple, 0, 4000);
        memset(returnedData, 0, 4000);
        prepareLargeTuple(attrs.size(), nullsIndicator, i + 10, tuple, &size);
        rc = rm.readTuple(tableName, rids[i], returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");

        if (memcmp(returnedData, tuple, sizes[i]) != 0) {
            std::cout << "***** [FAIL] Test Case 10 Failed *****" << std::endl << std::endl;
            free(tuple);
            free(returnedData);
            free(nullsIndicator);
            return -1;
        }
    }

    free(tuple);
    free(returnedData);
    free(nullsIndicator);

    std::cout << "***** Test Case 10 Finished. The result will be examined. *****" << std::endl << std::endl;

    return success;

}

int main() {
    std::vector<RID> rids;
    std::vector<int> sizes;

    // Update Tuple
    return TEST_RM_10("tbl_employee4", rids, sizes);
}

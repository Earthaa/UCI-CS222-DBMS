#include "rm_test_util.h"

RC TEST_RM_8(const std::string &tableName, std::vector<RID> &rids, std::vector<int> &sizes) {
    // Functions Tested for large tables:
    // 1. getAttributes
    // 2. insert tuple
    std::cout << std::endl << "***** In RM Test Case 8 *****" << std::endl;

    RID rid;
    void *tuple = malloc(4000);
    int numTuples = 2000;

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Insert 2000 tuples into table
    for (int i = 0; i < numTuples; i++) {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 2000);
        prepareLargeTuple(attrs.size(), nullsIndicator, i, tuple, &size);

        rc = rm.insertTuple(tableName, tuple, rid);
        assert(rc == success && "RelationManager::insertTuple() should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);
    }

    free(tuple);
    free(nullsIndicator);

    writeRIDsToDisk(rids);
    writeSizesToDisk(sizes);

    std::cout << "***** Test Case 8 Finished. The result will be examined. *****" << std::endl << std::endl;

    return success;
}

int main() {
    std::vector<RID> rids;
    std::vector<int> sizes;

    // Insert Tuple
    return TEST_RM_8("tbl_employee4", rids, sizes);
}

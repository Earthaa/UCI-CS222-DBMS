#include "qe_test_util.h"

RC testCase_15() {
    // Optional for all: +5 extra credit points will be given based on the results of the group-based hash aggregation related tests.
    // Aggregate -- MIN (with GroupBy)
    // SELECT group.B, MIN(group.A) FROM group GROUP BY group.B

    std::cerr << "***** In QE Test Case 15 *****" << std::endl;

    RC rc = 0;
    // Create TableScan
    auto *input = new TableScan(rm, "group");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "group.A";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "group.B";
    gAttr.type = TypeInt;
    gAttr.length = 4;
    auto *agg = new Aggregate(input, aggAttr, gAttr, MIN);

    int idVal = 0;
    float minVal = 0;
    int expectedResultCnt = 5;
    int actualResultCnt = 0;

    void *data = malloc(bufSize);

    while (agg->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Print group.B
        idVal = *(int *) ((char *) data + offset + 1);
        std::cerr << "group.B " << idVal;
        offset += sizeof(int);

        // Print MIN(group.A)
        minVal = *(float *) ((char *) data + offset + 1);
        std::cerr << "  MIN(group.A) " << minVal << std::endl;

        memset(data, 0, bufSize);
        if (idVal != (int) minVal) {
            std::cerr << "***** The group-based aggregation is not working properly. *****" << std::endl;
            rc = fail;
            break;
        }
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
        rc = fail;
    }

    delete agg;
    delete input;
    free(data);
    return rc;
}

int main() {

    if (createGroupTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 15 failed. *****" << std::endl;
        return fail;
    }

    if (populateGroupTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 15 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_15() != success) {
        std::cerr << "***** [FAIL] QE Test Case 15 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 15 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

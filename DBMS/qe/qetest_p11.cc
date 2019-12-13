#include "qe_test_util.h"

RC privateTestCase_11() {
    // Optional for all
    // (+5 extra credit points will be given based on the results of the group-based hash aggregation related tests)
    // Aggregate -- MAX (with GroupBy)
    // SELECT group.B, MAX(group.C) FROM group where group.B > 3 GROUP BY group.B

    std::cout << std::endl << "***** In QE Private Test Case 11 *****" << std::endl;

    RC rc = 0;

    // Create IndexScan
    auto *input = new IndexScan(rm, "group", "B");

    int compVal = 3; // group.B should be 4,5

    // Set up condition
    Condition cond2;
    cond2.lhsAttr = "group.B";
    cond2.op = GT_OP;
    cond2.bRhsIsAttr = false;
    Value value2{};
    value2.type = TypeInt;
    value2.data = malloc(bufSize);
    *(int *) value2.data = compVal;
    cond2.rhsValue = value2;

    // Create Filter
    auto *filter = new Filter(input, cond2);

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "group.C";
    aggAttr.type = TypeReal;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "group.B";
    gAttr.type = TypeInt;
    gAttr.length = 4;
    auto *agg = new Aggregate(filter, aggAttr, gAttr, MAX);

    int idVal = 0;
    float maxVal = 0;
    int expectedResultCnt = 2;
    int actualResultCnt = 0;

    void *data = malloc(bufSize);

    while (agg->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Print group.B
        idVal = *(int *) ((char *) data + offset + 1);
        std::cout << "group.B " << idVal;
        offset += sizeof(int);

        // Print MAX(group.A)
        maxVal = *(float *) ((char *) data + offset + 1);
        std::cout << "  MAX(group.C) " << maxVal << std::endl;

        memset(data, 0, bufSize);
        if ((idVal == 4 && maxVal != 148) || (idVal == 5 && maxVal != 149)) {
            std::cout << "***** The group-based aggregation is not working properly. *****" << std::endl;
            rc = fail;
            break;
        }
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cout << "***** The number of returned tuple: " << actualResultCnt << " is not correct. *****" << std::endl;
        rc = fail;
    }

    delete agg;
    delete filter;
    delete input;
    free(data);
    return rc;
}

int main() {
    // Indexes created: group.B

    if (createIndexforGroupB() != success) {
        std::cout << "***** [FAIL] QE Private Test Case 11 failed. *****" << std::endl;
        return fail;
    }

    if (privateTestCase_11() != success) {
        std::cout << "***** [FAIL] QE Private Test Case 11 failed. *****" << std::endl;
        return fail;
    } else {
        std::cout << "***** QE Private Test Case 11 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

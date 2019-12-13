#include "qe_test_util.h"

RC privateTestCase_9() {
    // Mandatory for the grad teams/solos
    // Optional for the undergrad solo
    // (+5 extra credit points will be given based on the results of the basic aggregation related tests)
    // 1. Basic aggregation - MIN
    // SELECT MIN(largeleft.B) from largeleft
    std::cerr << std::endl << "***** In QE Private Test Case 9 *****" << std::endl;

    RC rc = success;

    int compVal = 1000;

    // Create IndexScan
    auto *input = new TableScan(rm, "largeleft");

    // Set up condition
    Condition cond2;
    cond2.lhsAttr = "largeleft.B";
    cond2.op = LT_OP;
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
    aggAttr.name = "largeleft.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    auto *agg = new Aggregate(filter, aggAttr, MIN);

    int count = 0;
    void *data = malloc(bufSize);

    // An aggregation returns a float value
    float minVal = 0.0;

    while (agg->getNextTuple(data) != QE_EOF) {
        minVal = *(float *) ((char *) data + 1);
        std::cerr << "MIN(largeleft.B) " << minVal << std::endl;
        memset(data, 0, sizeof(int));
        count++;
        if (count > 1) {
            std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
            rc = fail;
            break;
        }
    }

    if (minVal != 10.0) {
        std::cerr << "***** The returned value: " << minVal << " is not correct. *****" << std::endl;
        rc = fail;
    }

    delete agg;
    delete filter;
    delete input;
    free(data);
    return rc;

}

int main() {

    if (privateTestCase_9() != success) {
        std::cerr << "***** [FAIL] QE Test Case 9 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Private Test Case 9 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

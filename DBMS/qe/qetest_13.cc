#include "qe_test_util.h"

RC testCase_13() {
    // Mandatory for the grad teams/solos
    // Optional for undergrad solos: +5 extra credit points will be given based on the results of the basic aggregation related tests
    // 1. Basic aggregation - max
    // SELECT max(left.B) from left
    std::cerr << "***** In QE Test Case 13 *****" << std::endl;

    RC rc = success;

    // Create TableScan
    auto *input = new TableScan(rm, "left");

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "left.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    auto *agg = new Aggregate(input, aggAttr, MAX);

    int count = 0;
    void *data = malloc(bufSize);

    // An aggregation returns a float value
    float maxVal = 0.0;

    while (agg->getNextTuple(data) != QE_EOF) {
        maxVal = *(float *) ((char *) data + 1);
        std::cerr << "MAX(left.B) " << maxVal << std::endl;
        memset(data, 0, sizeof(int));
        count++;
        if (count > 1) {
            std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
            rc = fail;
            break;
        }
    }

    if (maxVal != 109.0) {
        rc = fail;
    }

    delete agg;
    delete input;
    free(data);
    return rc;

}

int main() {

    if (testCase_13() != success) {
        std::cerr << "***** [FAIL] QE Test Case 13 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 13 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

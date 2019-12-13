#include "qe_test_util.h"

RC testCase_14() {
    // Mandatory for the grad teams/solos
    // Optional for the undergrad solos: +5 extra credit points will be given based on the results of the basic aggregation related tests.
    // 1. Basic aggregation - AVG
    // SELECT AVG(right.B) from left

    std::cerr << "***** In QE Test Case 14 *****" << std::endl;

    // Create TableScan
    auto *input = new TableScan(rm, "right");

    RC rc = success;

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "right.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    auto *agg = new Aggregate(input, aggAttr, AVG);

    void *data = malloc(bufSize);
    float average = 0.0;
    int count = 0;

    while (agg->getNextTuple(data) != QE_EOF) {
        average = *(float *) ((char *) data + 1);
        std::cerr << "AVG(right.B) " << average << std::endl;
        memset(data, 0, sizeof(float) + 1);
        count++;
        if (count > 1) {
            std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
            rc = fail;
            break;
        }
    }

    if (average != 69.5) {
        rc = fail;
    }

    delete agg;
    delete input;
    free(data);
    return rc;
}

int main() {

    if (testCase_14() != success) {
        std::cerr << "***** [FAIL] QE Test Case 14 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 14 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

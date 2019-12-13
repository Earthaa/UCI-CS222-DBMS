#include "qe_test_util.h"

RC testCase_2() {
    // Mandatory for all
    // Create an Index
    // Load Data
    // Create an Index

    RC rc;
    std::cerr << std::endl << "***** In QE Test Case 2 *****" << std::endl;

    rc = createIndexforRightB();
    if (rc != success) {
        std::cerr << "***** createIndexforRightB() failed.  *****" << std::endl;
        return rc;
    }

    rc = populateRightTable();
    if (rc != success) {
        std::cerr << "***** populateRightTable() failed.  *****" << std::endl;
        return rc;
    }

    rc = createIndexforRightC();
    if (rc != success) {
        std::cerr << "***** createIndexforRightC() failed.  *****" << std::endl;
        return rc;
    }

    return rc;
}

int main() {
    // Tables created: right
    // Indexes created: right.B, right.C

    // Create the right table
    if (createRightTable() != success) {
        std::cerr << "***** createRightTable() failed. *****" << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_2() != success) {
        std::cerr << "***** [FAIL] QE Test Case 2 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 2 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

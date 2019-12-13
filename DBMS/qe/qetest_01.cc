#include "qe_test_util.h"

int testCase_1() {
    // Mandatory for all
    // Create an Index
    // Load Data
    // Create an Index

    RC rc;
    std::cerr << std::endl << "***** In QE Test Case 1 *****" << std::endl;

    // Create an index before inserting tuples.
    rc = createIndexforLeftB();
    if (rc != success) {
        std::cerr << "***** createIndexforLeftB() failed.  *****" << std::endl;
        return rc;
    }

    // Insert tuples.
    rc = populateLeftTable();
    if (rc != success) {
        std::cerr << "***** populateLeftTable() failed.  *****" << std::endl;
        return rc;
    }

    // Create an index after inserting tuples - should reflect the currently existing tuples.
    rc = createIndexforLeftC();
    if (rc != success) {
        std::cerr << "***** createIndexforLeftC() failed.  *****" << std::endl;
        return rc;
    }
    return rc;
}

int main() {
    // Tables created: left
    // Indexes created: left.B, left.C

    // Initialize the system catalog
    if (deleteAndCreateCatalog() != success) {
        std::cerr << "***** deleteAndCreateCatalog() failed." << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    // Create the left table
    if (createLeftTable() != success) {
        std::cerr << "***** createLeftTable() failed." << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_1() != success) {
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 1 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

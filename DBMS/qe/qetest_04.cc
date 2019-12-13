#include "qe_test_util.h"

RC exitWithError(const TableScan *ts, const Value &value, const Filter *filter, void *data) {
    delete filter;
    delete ts;
    free(data);
    free(value.data);
    return fail;
}

int testCase_4() {
    // Mandatory for all
    // 1. Filter -- on TypeVarChar Attribute
    // SELECT * FROM leftvarchar where B = "llllllllllll"
    std::cerr << std::endl << "***** In QE Test Case 4 *****" << std::endl;

    RC rc = success;
    auto *ts = new TableScan(rm, "leftvarchar");

    // Set up condition
    Condition cond;
    cond.lhsAttr = "leftvarchar.B";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = false;
    Value value{};
    value.type = TypeVarChar;
    value.data = malloc(bufSize);
    int length = 12;
    *(int *) ((char *) value.data) = length;
    for (unsigned i = 0; i < 12; ++i) {
        *(char *) ((char *) value.data + 4 + i) = 12 + 96;
    }
    cond.rhsValue = value; // "llllllllllll"

    // Create Filter
    auto *filter = new Filter(ts, cond);

    int expectedResultCnt = 39;
    int actualResultCnt = 0;

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (filter->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Null indicators should be placed in the beginning.

        // Is an attribute A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(ts, value, filter, data);
        }

        // Print leftvarchar.A
        std::cerr << "leftvarchar.A " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(ts, value, filter, data);
        }

        // Print leftvarchar.B
        int len = *(int *) ((char *) data + offset + 1);
        offset += 4;
        std::cerr << "  leftvarchar.B.length " << len;

        char *b = (char *) malloc(100);
        memcpy(b, (char *) data + offset + 1, len);
        b[len] = '\0';
        std::cerr << "  leftvarchar.B " << b << std::endl;

        memset(data, 0, bufSize);
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
        return exitWithError(ts, value, filter, data);
    }
    delete filter;
    delete ts;
    free(data);
    free(value.data);
    return rc;
}

int main() {
    // Tables created: leftvarchar, rightvarchar
    // Indexes created: none

    // Create left/right large table, and populate the two tables
    if (createLeftVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (populateLeftVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (createRightVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (populateRightVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_4() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 4 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

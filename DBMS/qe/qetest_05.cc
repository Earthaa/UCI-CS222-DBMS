#include "qe_test_util.h"

RC exitWithError(const IndexScan *is, const Value &value, const Filter *filter, void *data) {
    delete filter;
    delete is;
    free(data);
    free(value.data);
    return fail;
}

RC testCase_5() {
    // Mandatory for all
    // 1. Filter -- IndexScan as input, on TypeReal attribute
    // SELECT * FROM RIGHT WHERE C >= 110.0
    cerr << endl << "***** In QE Test Case 5 *****" << endl;

    RC rc = success;
    auto *is = new IndexScan(rm, "right", "C");
    float compVal = 110.0;
    float valueC = 0;

    // Set up condition
    Condition cond;
    cond.lhsAttr = "right.C";
    cond.op = GE_OP;
    cond.bRhsIsAttr = false;
    Value value{};
    value.type = TypeReal;
    value.data = malloc(bufSize);
    *(float *) value.data = compVal;
    cond.rhsValue = value;

    int expectedResultCnt = 15; // 110.00 ~ 124.00;
    int actualResultCnt = 0;

    // Create Filter
    auto *filter = new Filter(is, cond);

    // Go over the data through iterator
    void *data = malloc(bufSize);

    bool nullBit;

    int valueB = 0;
    int valueD = 0;

    while (filter->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // is an attribute B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            cerr << endl << "***** A returned value is not correct. *****" << endl;
            return exitWithError(is, value, filter, data);
        }
        valueB = *(int *) ((char *) data + 1 + offset);

        // Print right.B
        cerr << "right.B " << valueB;
        offset += sizeof(int);

        // is an attribute C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            cerr << endl << "***** A returned value is not correct. *****" << endl;
            return exitWithError(is, value, filter, data);
        }
        valueC = *(float *) ((char *) data + 1 + offset);

        // Print right.C
        cerr << "  right.C " << valueC;
        offset += sizeof(float);
        if (valueC < compVal) {
            return exitWithError(is, value, filter, data);
        }

        // is an attribute D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            cerr << endl << "***** A returned value is not correct. *****" << endl;
            return exitWithError(is, value, filter, data);
        }
        valueD = *(int *) ((char *) data + 1 + offset);

        // Print right.D
        cerr << "  right.D " << valueD << endl;

        memset(data, 0, bufSize);
        actualResultCnt++;
    }
    if (expectedResultCnt != actualResultCnt) {
        cerr << "***** The number of returned tuple is not correct. *****" << endl;
        return exitWithError(is, value, filter, data);
    }
    delete filter;
    delete is;
    free(data);
    free(value.data);
    return rc;
}

int main() {
    // Tables created: none
    // Indexes created: none

    if (testCase_5() != success) {
        cerr << "***** [FAIL] QE Test Case 5 failed. *****" << endl;
        return fail;
    } else {
        cerr << "***** QE Test Case 5 finished. The result will be examined. *****" << endl;
        return success;
    }
}

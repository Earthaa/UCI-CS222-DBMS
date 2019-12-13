#include "qe_test_util.h"

RC privateTestCase_2() {
    // Function Tested
    // Insert records into table with NULL values
    // Table scan with filter
    // Index scan with filter
    // compare the results from both scans
    std::cerr << std::endl << "***** In QE Private Test Case 2 *****" << std::endl;
    RC rc = success;

    auto *ts = new TableScan(rm, "left2");
    int compVal = 50;

    // Set up condition
    Condition cond1;
    cond1.lhsAttr = "left2.B";
    cond1.op = LT_OP;
    cond1.bRhsIsAttr = false;
    Value value1{};
    value1.type = TypeInt;
    value1.data = malloc(bufSize);
    *(int *) value1.data = compVal;
    cond1.rhsValue = value1;

    // Create Filter
    auto *filter1 = new Filter(ts, cond1);

    auto *is = new IndexScan(rm, "left2", "B");

    // Set up condition
    Condition cond2;
    cond2.lhsAttr = "left2.B";
    cond2.op = LT_OP;
    cond2.bRhsIsAttr = false;
    Value value2{};
    value2.type = TypeInt;
    value2.data = malloc(bufSize);
    *(int *) value2.data = compVal;
    cond2.rhsValue = value2;

    // Create Filter
    auto *filter2 = new Filter(is, cond2);

    // Go over the data through iterator
    char data1[bufSize];
    char data2[bufSize];
    int offset;
    memset(data1, 0, bufSize);
    memset(data2, 0, bufSize);
    int count = 0;
    bool nullBit1;
    bool nullBit2;
    int nullCount1 = 0;
    int nullCount2 = 0;
    int tscount = 0;
    int iscount = 0;

    while (filter1->getNextTuple(data1) != QE_EOF) {
        tscount++;

        if (filter2->getNextTuple(data2) == QE_EOF) {
            std::cerr << " ***** [FAIL] The numbers of results: " << count << " " << tscount << " " << iscount
                      << " from both scan do not match. ***** " << std::endl;
            rc = fail;
            break;
        } else {
            iscount++;
        }

        offset = 0; // including null indicators

        // Compare field A value
        nullBit1 = *(unsigned char *) ((char *) data1 + offset) & ((unsigned) 1 << (unsigned) 7);
        nullBit2 = *(unsigned char *) ((char *) data2 + offset) & ((unsigned) 1 << (unsigned) 7);

        if (nullBit1) {
            nullCount1++;
        }

        if (nullBit2) {
            nullCount2++;
        }

        memset(data1, 0, bufSize);
        memset(data2, 0, bufSize);

        count++;
    }

    // left.B [10-109] : 40
    if (count != 40 || nullCount1 != 20 || nullCount2 != 20) {
        std::cerr << "***** [FAIL] The number of result: " << count << " " << nullCount1 << " " << nullCount2
                  << " is not correct. ***** " << std::endl;
        rc = fail;
    }

    delete filter1;
    delete filter2;
    delete ts;
    delete is;
    free(value1.data);
    free(value2.data);
    return rc;
}

int main() {
    // Tables created: left2
    // Indexes created: left2.B

    if (createLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftB2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (populateLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (privateTestCase_2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Private Test Case 2 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

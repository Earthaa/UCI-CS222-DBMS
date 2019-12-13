#include "qe_test_util.h"

RC privateTestCase_0() {
    // Function Tested
    // Insert three records into table
    // update two records
    // delete the last record
    // Table scan with filter
    // Index scan with filter
    // compare the results from both scans
    std::cerr << std::endl << "***** In QE Private Test Case 0 *****" << std::endl;
    RC rc = success;

    auto *ts = new TableScan(rm, "left3");
    int compVal = 100;

    // Set up condition
    Condition cond1;
    cond1.lhsAttr = "left3.B";
    cond1.op = GE_OP;
    cond1.bRhsIsAttr = false;
    Value value1{};
    value1.type = TypeInt;
    value1.data = malloc(bufSize);
    *(int *) value1.data = compVal;
    cond1.rhsValue = value1;

    // Create Filter
    auto *filter1 = new Filter(ts, cond1);

    auto *is = new IndexScan(rm, "left3", "B");

    // Set up condition
    Condition cond2;
    cond2.lhsAttr = "left3.B";
    cond2.op = GE_OP;
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
    memset(data1, 0, bufSize);
    memset(data2, 0, bufSize);
    int count = 0;
    int tscount = 0;
    int iscount = 0;

    while (filter1->getNextTuple(data1) != QE_EOF) {
        tscount++;

        if (filter2->getNextTuple(data2) == QE_EOF) {
            std::cerr << "***** [FAIL] The numbers of results from both scan: " << count << " " << tscount << " "
                      << iscount << " do not match. ***** " << std::endl;
            rc = fail;
            break;
        } else {
            iscount++;
        }

        memset(data1, 0, bufSize);
        memset(data2, 0, bufSize);

        count++;
    }

    // largeleft.B >= 100 from [110-111] = 2
    if (count != 2 || tscount != count || iscount != count) {
        std::cerr << " ***** [FAIL] The number of result: " << count << " is not correct. ***** " << std::endl;
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
    // Tables created: left3
    // Indexes created: left3.B

    if (createLeftTable3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftB3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    std::vector<RID> rids;

    if (populateLeftTable3(rids) != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftC3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (updateLeftTable3(rids) != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (privateTestCase_0() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Private Test Case 0 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

#include "qe_test_util.h"

RC exitWithError(const TableScan *leftIn, const TableScan *rightIn, const BNLJoin *bnlJoin, const Value &value,
                 const Filter *filter, void *data) {
    delete filter;
    delete bnlJoin;
    delete leftIn;
    delete rightIn;
    free(value.data);
    free(data);
    return fail;
}

RC testCase_8() {
    // Mandatory for - grad teams/solos
    // Optional for - undergrad solo (+5 extra credit points will be given based on the results of the BNLJ related tests)
    // Functions Tested
    // 1. BNLJoin -- on TypeInt Attribute
    // 2. Filter -- on TypeInt Attribute
    // SELECT * FROM left, right WHERE left.B = right.B AND right.B >= 100
    std::cerr << std::endl << "***** In QE Test Case 8 *****" << std::endl;

    RC rc = success;

    // Prepare the iterator and condition
    auto *leftIn = new TableScan(rm, "left");
    auto *rightIn = new TableScan(rm, "right");

    Condition cond_j;
    cond_j.lhsAttr = "left.B";
    cond_j.op = EQ_OP;
    cond_j.bRhsIsAttr = true;
    cond_j.rhsAttr = "right.B";

    // Create NLJoin
    auto *bnlJoin = new BNLJoin(leftIn, rightIn, cond_j, 5);

    int compVal = 100;

    // Create Filter
    Condition cond_f;
    cond_f.lhsAttr = "right.B";
    cond_f.op = GE_OP;
    cond_f.bRhsIsAttr = false;
    Value value{};
    value.type = TypeInt;
    value.data = malloc(bufSize);
    *(int *) value.data = compVal;
    cond_f.rhsValue = value;

    int expectedResultCnt = 10; // join result: [20,109] --> filter result [100, 109]
    int actualResultCnt = 0;
    int valueB = 0;

    auto *filter = new Filter(bnlJoin, cond_f);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (filter->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute left.A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print left.A
        std::cerr << "left.A " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute left.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print left.B
        valueB = *(int *) ((char *) data + offset + 1);
        std::cerr << "left.B " << valueB << std::endl;
        offset += sizeof(int);
        if (valueB < 100 || valueB > 109) {
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }

        // Is an attribute left.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print left.C
        std::cerr << "left.C " << *(float *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(float);

        // Is an attribute right.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print right.B
        std::cerr << "right.B " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print right.C
        std::cerr << "right.C " << *(float *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(float);

        // Is an attribute right.D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 2);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, value, filter, data);
        }
        // Print right.D
        std::cerr << "right.D " << *(int *) ((char *) data + offset + 1) << std::endl;

        memset(data, 0, bufSize);
        ++actualResultCnt;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
        rc = fail;
    }

    delete filter;
    delete bnlJoin;
    delete leftIn;
    delete rightIn;
    free(value.data);
    free(data);
    return rc;
}

int main() {

    if (testCase_8() != success) {
        std::cerr << "***** [FAIL] QE Test Case 8 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 8 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

#include "qe_test_util.h"

RC exitWithError(const TableScan *leftIn, const TableScan *rightIn, const BNLJoin *bnlJoin, void *data) {
    delete bnlJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return fail;
}

RC testCase_7() {
    // Mandatory for grad teams/solos
    // Optional for - undergrad solo (+5 extra credit points will be given based on the results of the BNLJ related tests)
    // 1. BNLJoin -- on TypeInt Attribute
    // SELECT * FROM left, right where left.B = right.B
    std::cerr << std::endl << "***** In QE Test Case 7 *****" << std::endl;

    RC rc = success;
    // Prepare the iterator and condition
    auto leftIn = new TableScan(rm, "left");
    auto *rightIn = new TableScan(rm, "right");

    Condition cond;
    cond.lhsAttr = "left.B";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.B";

    int expectedResultCnt = 90; //20~109 --> left.B: [10,109], right.B: [20,119]
    int actualResultCnt = 0;
    int valueB = 0;

    // Create BNLJoin
    auto *bnlJoin = new BNLJoin(leftIn, rightIn, cond, 5);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (bnlJoin->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute left.A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }
        // Print left.A
        std::cerr << "left.A " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute left.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }
        // Print left.B
        std::cerr << "left.B " << *(int *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(int);

        // Is an attribute left.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }
        // Print left.C
        std::cerr << "left.C " << *(float *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(float);

        // Is an attribute right.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }
        // Print right.B
        valueB = *(int *) ((char *) data + offset + 1);
        std::cerr << "right.B " << valueB << std::endl;
        offset += sizeof(int);

        if (valueB < 20 || valueB > 109) {
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
        }
        // Print right.C
        std::cerr << "right.C " << *(float *) ((char *) data + offset + 1) << std::endl;
        offset += sizeof(float);

        // Is an attribute right.D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 2);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, bnlJoin, data);
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

    delete bnlJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return rc;
}

int main() {

    if (testCase_7() != success) {
        std::cerr << "***** [FAIL] QE Test Case 7 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 7 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

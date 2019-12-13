#include "qe_test_util.h"

RC exitWithError(const TableScan *leftIn, const IndexScan *rightIn, const INLJoin *inlJoin, void *data) {
    delete inlJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return fail;
}

RC testCase_9() {
    // Mandatory for all
    // 1. INLJoin -- on TypeReal Attribute
    // SELECT * from left, right WHERE left.C = right.C
    std::cerr << std::endl << "***** In QE Test Case 9 *****" << std::endl;

    RC rc = success;

    // Prepare the iterator and condition
    auto *leftIn = new TableScan(rm, "left");
    auto *rightIn = new IndexScan(rm, "right", "C");

    Condition cond;
    cond.lhsAttr = "left.C";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.C";

    int expectedResultCnt = 75; // 50.0~124.0  left.C: [50.0,149.0], right.C: [25.0,124.0]
    int actualResultCnt = 0;
    float valueC = 0;

    // Create INLJoin
    auto *inlJoin = new INLJoin(leftIn, rightIn, cond);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (inlJoin->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute left.A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print left.A
        std::cerr << "left.A " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute left.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print left.B
        std::cerr << "  left.B " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute left.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print left.C
        std::cerr << "  left.C " << *(float *) ((char *) data + offset + 1);
        offset += sizeof(float);

        // Is an attribute right.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print right.B
        std::cerr << "  right.B " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print right.C
        valueC = *(float *) ((char *) data + offset + 1);
        std::cerr << "  right.C " << valueC;
        offset += sizeof(float);
        if (valueC < 50.0 || valueC > 124.0) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 2);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, rightIn, inlJoin, data);
        }
        // Print right.D
        std::cerr << "  right.D " << *(int *) ((char *) data + offset + 1) << std::endl;

        memset(data, 0, bufSize);
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
        rc = fail;
    }

    delete inlJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return rc;
}

int main() {

    if (testCase_9() != success) {
        std::cerr << "***** [FAIL] QE Test Case 9 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 9 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

#include "qe_test_util.h"

RC exitWithError(const TableScan *leftIn, const TableScan *rightIn, const GHJoin *ghJoin, void *data) {
    delete ghJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return fail;
}

RC testCase_12() {
    // Optional for all: +10 extra credit points will be given based on the results of the GHJ related tests.
    // 1. GHJoin -- on TypeReal Attribute
    // SELECT * from largeleft, largeright WHERE largeleft.C = largeright.C
    std::cerr << std::endl << "***** In QE Test Case 12 *****" << std::endl;

    RC rc = success;

    // Prepare the iterator and condition
    auto *leftIn = new TableScan(rm, "largeleft");
    auto *rightIn = new TableScan(rm, "largeright");

    Condition cond;
    cond.lhsAttr = "largeleft.C";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "largeright.C";

    // int expectedResultCnt = 49975; // 50.0~50024.0  left.C: [50.0,50049.0], right.C: [25.0,50024.0]
    int actualResultCnt = 0;
    float valueC = 0;
    int numPartitons = 10;

    // Create GHJoin
    auto *ghJoin = new GHJoin(leftIn, rightIn, cond, numPartitons);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (ghJoin->getNextTuple(data) != QE_EOF) {

        // At this point, partitions should be on disk.

        if (actualResultCnt % 5000 == 0) {
            std::cerr << "Processing " << actualResultCnt << " of " << largeTupleCount << " tuples." << std::endl;
            int offset = 0;
            // Is an attribute left.A NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print left.A
            std::cerr << "largeleft.A " << *(int *) ((char *) data + offset + 1);
            offset += sizeof(int);

            // Is an attribute left.B NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print left.B
            std::cerr << "  largeleft.B " << *(int *) ((char *) data + offset + 1);
            offset += sizeof(int);

            // Is an attribute left.C NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print left.C
            std::cerr << "  largeleft.C " << *(float *) ((char *) data + offset + 1);
            offset += sizeof(float);

            // Is an attribute right.B NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print right.B
            std::cerr << "  largeright.B " << *(int *) ((char *) data + offset + 1);
            offset += sizeof(int);

            // Is an attribute right.C NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print right.C
            valueC = *(float *) ((char *) data + offset + 1);
            std::cerr << "  largeright.C " << valueC;
            offset += sizeof(float);
            if (valueC < 50.0 || valueC > 50024.0) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }

            // Is an attribute right.D NULL?
            nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
            if (nullBit) {
                std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
                return exitWithError(leftIn, rightIn, ghJoin, data);
            }
            // Print right.D
            std::cerr << "  largeright.D " << *(int *) ((char *) data + offset + 1) << std::endl;
        }

        memset(data, 0, bufSize);
        actualResultCnt++;

        if (actualResultCnt == 10000) {
            std::cerr << std::endl
                      << "***** Stopping the join process on purpose. The contents in the directory will be checked. *****"
                      << std::endl;
            break;
        }
    }

    delete ghJoin;
    delete leftIn;
    delete rightIn;
    free(data);
    return rc;
}

int main() {

    if (testCase_12() != success) {
        std::cerr << "***** [FAIL] QE Test Case 12 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr
                << "***** QE Test Case 12 finished. The result and the contents in the directory will be examined. *****"
                << std::endl;
        return success;
    }
}

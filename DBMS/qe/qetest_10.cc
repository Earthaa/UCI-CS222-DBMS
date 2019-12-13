#include "qe_test_util.h"

RC exitWithError(const IndexScan *leftIn, const Value &value, const Filter *filter, const Project *project,
                 const IndexScan *rightIn, const Iterator *join, void *data) {
    delete join;
    delete rightIn;
    delete project;
    delete filter;
    delete leftIn;
    free(value.data);
    free(data);
    return fail;
}

int testCase_10() {
    // Mandatory for all
    // 1. Filter
    // 2. Project
    // 3. INLJoin
    // SELECT A1.A, A1.C, right.* FROM (SELECT * FROM left WHERE left.B < 75) A1, right WHERE A1.C = right.C
    std::cerr << std::endl << "***** In QE Test Case 10 *****" << std::endl;

    RC rc = success;
    // Create Filter
    auto *leftIn = new IndexScan(rm, "left", "B");

    int compVal = 75;

    Condition cond_f;
    cond_f.lhsAttr = "left.B";
    cond_f.op = LT_OP;
    cond_f.bRhsIsAttr = false;
    Value value{};
    value.type = TypeInt;
    value.data = malloc(bufSize);
    *(int *) value.data = compVal;
    cond_f.rhsValue = value;

    leftIn->setIterator(NULL, value.data, true, false);
    auto *filter = new Filter(leftIn, cond_f); //left.B: 10~74, left.C: 50.0~114.0

    // Create Project
    std::vector<std::string> attrNames = {"left.A", "left.C"};
    auto *project = new Project(filter, attrNames);

    Condition cond_j;
    cond_j.lhsAttr = "left.C";
    cond_j.op = EQ_OP;
    cond_j.bRhsIsAttr = true;
    cond_j.rhsAttr = "right.C";

    // Create Join
    IndexScan *rightIn = NULL;
    Iterator *join = NULL;
    rightIn = new IndexScan(rm, "right", "C");
    join = new INLJoin(project, rightIn, cond_j);

    int expectedResultCnt = 65; //50.0~114.0
    int actualResultCnt = 0;
    float valueC = 0;

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (join->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute left.A NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }
        // Print left.A
        std::cerr << "left.A " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute left.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }
        // Print left.C
        std::cerr << "  left.C " << *(float *) ((char *) data + offset + 1);
        offset += sizeof(float);

        // Is an attribute right.B NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 5);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }
        // Print right.B
        std::cerr << "  right.B " << *(int *) ((char *) data + offset + 1);
        offset += sizeof(int);

        // Is an attribute right.C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 4);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }
        // Print right.C
        valueC = *(float *) ((char *) data + offset + 1);
        std::cerr << "  right.C " << valueC;
        offset += sizeof(float);
        if (valueC < 50.0 || valueC > 114.0) {
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }

        // Is an attribute right.D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 3);
        if (nullBit) {
            std::cerr << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(leftIn, value, filter, project, rightIn, join, data);
        }
        // Print right.D
        std::cerr << "  right.D " << *(int *) ((char *) data + offset + 1);

        memset(data, 0, bufSize);
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cerr << "***** The number of returned tuple is not correct. *****" << std::endl;
        rc = fail;
    }

    delete join;
    delete rightIn;
    delete project;
    delete filter;
    delete leftIn;
    free(value.data);
    free(data);
    return rc;
}

int main() {

    if (testCase_10() != success) {
        std::cerr << "***** [FAIL] QE Test Case 10 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 10 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

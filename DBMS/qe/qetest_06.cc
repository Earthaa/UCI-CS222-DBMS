#include "qe_test_util.h"

RC exitWithError(const TableScan *ts, const Project *project, void *data) {
    delete project;
    delete ts;
    free(data);
    return fail;
}

RC testCase_6() {
    // Mandatory for all
    // Project -- TableScan as input
    // SELECT C,D FROM RIGHT
    std::cout << std::endl << "***** In QE Test Case 6 *****" << std::endl;

    RC rc = success;
    auto *ts = new TableScan(rm, "right");

    std::vector<std::string> attrNames;
    attrNames.emplace_back("right.C");
    attrNames.emplace_back("right.D");

    int expectedResultCnt = 100;
    int actualResultCnt = 0;
    float valueC;
    int valueD = 0;

    // Create Projector
    auto *project = new Project(ts, attrNames);

    // Go over the data through iterator
    void *data = malloc(bufSize);
    bool nullBit;

    while (project->getNextTuple(data) != QE_EOF) {
        int offset = 0;

        // Is an attribute C NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 7);
        if (nullBit) {
            std::cout << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(ts, project, data);
        }
        valueC = *(float *) ((char *) data + 1 + offset);

        // Print right.C
        std::cout << "right.C " << valueC;
        offset += sizeof(float);


        // Is an attribute D NULL?
        nullBit = *(unsigned char *) ((char *) data) & ((unsigned) 1 << (unsigned) 6);
        if (nullBit) {
            std::cout << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(ts, project, data);
        }

        // Print right.D
        valueD = *(int *) ((char *) data + 1 + offset);
        std::cout << "  right.D " << valueD << std::endl;
        if (valueD < 0 || valueD > 99) {
            std::cout << std::endl << "***** A returned value is not correct. *****" << std::endl;
            return exitWithError(ts, project, data);
        }

        memset(data, 0, bufSize);
        actualResultCnt++;
    }

    if (expectedResultCnt != actualResultCnt) {
        std::cout << "***** The number of returned tuple is not correct. *****" << std::endl;
        rc = fail;
    }

    delete project;
    delete ts;
    free(data);
    return rc;
}

int main() {
    // Tables created: none
    // Indexes created: none

    if (testCase_6() != success) {
        std::cout << "***** [FAIL] QE Test Case 6 failed. *****" << std::endl;
        return fail;
    } else {
        std::cout << "***** QE Test Case 6 finished. The result will be examined. *****" << std::endl;
        return success;
    }
}

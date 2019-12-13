#include <iostream>
#include <cassert>
#include <stdexcept>
#include <cstdio>

#include "pfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_1(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Create File
    cout << endl << "***** In RBF Test Case 01 *****" << endl;

    RC rc;
    string fileName = "test1";

    // Create a file named "test"
    rc = pfm.createFile(fileName);
    assert(rc == success && "Creating the file failed.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Create "test" again, should fail
    rc = pfm.createFile(fileName);
    assert(rc != success && "Creating the same file should fail.");

    cout << "RBF Test Case 01 Finished! The result will be examined." << endl << endl;
    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    // Remove files that might be created by previous test run
    remove("test1");

    return RBFTest_1(pfm);
}

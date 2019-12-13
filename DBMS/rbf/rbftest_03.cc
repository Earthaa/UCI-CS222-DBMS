#include <iostream>
#include <cassert>
#include <stdexcept>
#include <cstdio>
#include "pfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_3(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Get Number Of Pages
    // 4. Close File
    cout << endl << "***** In RBF Test Case 03 *****" << endl;

    RC rc;
    string fileName = "test3";

    // Create a file named "test3"
    rc = pfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file
    FileHandle fileHandle;
    rc = pfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // Get the number of pages in the test file. In this case, it should be zero.
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned) 0 && "The page count should be zero at this moment.");

    // Close the file
    rc = pfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    cout << "RBF Test Case 03 Finished! The result will be examined." << endl << endl;

    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    remove("test3");

    return RBFTest_3(pfm);
}

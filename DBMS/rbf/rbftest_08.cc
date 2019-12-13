#include <iostream>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <cstdio>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_8(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 08 *****" << endl;

    RC rc;
    string fileName = "test8";

    // Create a file named "test8"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(nullsIndicator);
        free(record);
        free(returnedData);
        return -1;
    }

    cout << endl;

    // Close the file "test8"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");
    free(nullsIndicator);
    free(record);
    free(returnedData);

    cout << "RBF Test Case 08 Finished! The result will be examined." << endl << endl;

    return 0;
}

int main() {
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    remove("test8");

    return RBFTest_8(rbfm);
}

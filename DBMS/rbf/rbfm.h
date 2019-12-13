#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <stdio.h>
#include "pfm.h"
#include <cstring>
#include <iomanip>
#include <math.h> 
#include <iostream>


// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, pageData) != RBFM_EOF) {
//    process the pageData;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
    RBFM_ScanIterator() = default;;

    ~RBFM_ScanIterator() = default;;

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "pageData" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    RC close() {
        if(this->value != NULL)
            free(this->value);
        PagedFileManager::instance().closeFile(this->fileHandle); 
        return -1; };

    RC initScanIterator(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
            const std::string& conditionAttribute, const CompOp& compOp, const void* value,
            const std::vector<std::string>& attributeNames);
private:
    void increaseRID();
    bool isRIDValid();
    unsigned curPageNum = 0;
    unsigned curSlotNum = 0;
    FileHandle fileHandle;
    std::vector<Attribute> recordDescriptor;
    std::string conditionAttribute;
    CompOp compOp;
    std::vector<std::string> attributeNames;
    char* value = NULL;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the pageData passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual pageData part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual pageData is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned pageData of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    //Read an attribute without null byte
    RC readConvertedAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                              const RID &rid, const std::string &attributeName, void *data,
                              bool &isNull, int &attributeTrueLength);
    static RC readConvertedRecord(FileHandle &fileHandle, const RID &rid, unsigned &convertedRecordLength,
                        void *convertedRecordData);

protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    static RecordBasedFileManager *_rbf_manager;
    unsigned findAvailableExistedPage(FileHandle &fileHandle, unsigned dataLength);
    RC insertConvertedRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, 
                                    const char *convertedRecord, const unsigned recordLength, RID &rid);
};


class RecordPage{
    public:
        ~RecordPage();
        RecordPage(unsigned pageNum, const void* raw);
        unsigned getSlotData(const unsigned slotNum, void* data);
        char* getPageData();
        void getPageData(char* newData);
        unsigned getAvailableSlot(const unsigned dataLength);
        RC addNewRecord(unsigned slotNum, const unsigned recordLength, const char* record, FileHandle& fileHandle);
        RC deleteRecord(unsigned slotNum, FileHandle& fileHandle);
        RC updateRecord(unsigned slotNum, const unsigned recordLength, const char* record, FileHandle& fileHandle);
        void compact(int oldRecordLeftPoint, int delta);
        bool canContain(unsigned recordLength);
        int getFreeBytes();
        int getTotalSlots();
    private:
        char* pageData = NULL;
        unsigned pageNum;
        unsigned totalSlot; // Total slot is the number of all slots which include pointer slot, page slot and other page slot
        unsigned freeBytes;

};

class DataHelper{
    public:
        const static int indexSize = sizeof(unsigned);
        //To generate the new page by using one record, record should be a converted one
        static void getInitialRecordPage(const char* record, char* pageData, unsigned recordLength);
        static void rawToRecord(unsigned& recordLength, char *convertedData, const char *rawData, const std::vector<Attribute> &recordDescriptor);
        static void
        recordToRaw(const char *convertedData, char *rawData, const std::vector<Attribute> &recordDescriptor);
        static int getNullLength(const std::vector<Attribute> &recordDescriptor);
        static bool isNull(int index, char* nullByte);
        static bool isPointerRecord(const char *convertedData);
        static bool isRecordHere(const char* convertedData);
        static void createPointerRecord(char* data, unsigned& dataLength, const RID& rid);
        static void readRID(const char *convertedData, RID& rid);
        static void setNotHereMark(char * convertedData);
        static bool isCompareSatisfy(const void* arg1, const void* arg2, const CompOp& compOp, const AttrType& attrType);
        static void setNull(int index, char* nullByte);
};


#endif
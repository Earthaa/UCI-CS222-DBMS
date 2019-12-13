#ifndef _test_util_h_
#define _test_util_h_

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <cassert>
#include <ctime>
#include <sys/resource.h>
#include <set>
#include "rm.h"
#include "../rbf/test_util.h"

RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

RelationManager &rm = RelationManager::instance();

// This code is required for testing to measure the memory usage of your code.
// If you can't compile the codebase because of this function, you can safely comment this function or remove it.
void memProfile() {
    int who = RUSAGE_SELF;
    struct rusage usage{};
    getrusage(who, &usage);
    std::cerr << usage.ru_maxrss << "KB" << std::endl;
}

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int nameLength,
                  const std::string &name, const int age, const float height, const int salary, void *buffer,
                  unsigned *tupleSize) {
    unsigned offset = 0;

    // Null-indicators
    bool nullBit;
    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

    // Null-indicator for the fields
    memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // Beginning of the actual data
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

    // Is the name field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, name.c_str(), nameLength);
        offset += nameLength;
    }

    // Is the age field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &age, sizeof(int));
        offset += sizeof(int);
    }


    // Is the height field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &height, sizeof(float));
        offset += sizeof(float);
    }


    // Is the height field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &salary, sizeof(int));
        offset += sizeof(int);
    }

    *tupleSize = offset;
}

// Function to get the data in the correct form to be inserted/read after adding the attribute ssn
void prepareTupleAfterAdd(int attributeCount, unsigned char *nullAttributesIndicator, const int nameLength,
                          const std::string &name, const int age, const float height, const int salary, const int ssn,
                          void *buffer, unsigned *tupleSize) {
    unsigned offset = 0;

    // Null-indicators
    bool nullBit;
    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

    // Null-indicator for the fields
    memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // Beginning of the actual data
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

    // Is the name field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, name.c_str(), nameLength);
        offset += nameLength;
    }

    // Is the age field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &age, sizeof(int));
        offset += sizeof(int);
    }

    // Is the height field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &height, sizeof(float));
        offset += sizeof(float);
    }

    // Is the salary field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &salary, sizeof(int));
        offset += sizeof(int);
    }

    // Is the ssn field not-NULL?
    nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 3);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &ssn, sizeof(int));
        offset += sizeof(int);
    }

    *tupleSize = offset;
}

// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn
void prepareTupleAfterAdd(const int nameLength, const std::string &name, const int age, const float height,
                          const int salary, const int ssn, void *buffer, int *tupleSize) {
    int offset = 0;

    memcpy((char *) buffer + offset, &(nameLength), sizeof(int));
    offset += sizeof(int);
    memcpy((char *) buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *) buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *) buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *) buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    memcpy((char *) buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tupleSize = offset;
}

void printTupleAfterDrop(const void *buffer, const unsigned tupleSize) {
    unsigned offset = 0;
    std::cerr << "****Printing Buffer: Start****" << std::endl;

    int nameLength = 0;
    memcpy(&nameLength, (char *) buffer + offset, sizeof(int));
    offset += sizeof(int);
    std::cerr << "nameLength: " << nameLength << std::endl;

    char *name = (char *) malloc(100);
    memcpy(name, (char *) buffer + offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    std::cerr << "name: " << name << std::endl;

    int age = 0;
    memcpy(&age, (char *) buffer + offset, sizeof(int));
    offset += sizeof(int);
    std::cerr << "age: " << age << std::endl;

    float height = 0.0;
    memcpy(&height, (char *) buffer + offset, sizeof(float));
    std::cerr << "height: " << height << std::endl;

    std::cerr << "****Printing Buffer: End****" << std::endl << std::endl;
}

void printTupleAfterAdd(const void *buffer, const int tupleSize) {
    int offset = 0;
    std::cerr << "****Printing Buffer: Start****" << std::endl;

    int nameLength = 0;
    memcpy(&nameLength, (char *) buffer + offset, sizeof(int));
    offset += sizeof(int);
    std::cerr << "nameLength: " << nameLength << std::endl;

    char *name = (char *) malloc(100);
    memcpy(name, (char *) buffer + offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    std::cerr << "name: " << name << std::endl;

    int age = 0;
    memcpy(&age, (char *) buffer + offset, sizeof(int));
    offset += sizeof(int);
    std::cerr << "age: " << age << std::endl;

    float height = 0;
    memcpy(&height, (char *) buffer + offset, sizeof(float));
    offset += sizeof(float);
    std::cerr << "height: " << height << std::endl;

    int salary = 0;
    memcpy(&salary, (char *) buffer + offset, sizeof(int));
    offset += sizeof(int);
    std::cerr << "salary: " << salary << std::endl;

    int ssn = 0;
    memcpy(&ssn, (char *) buffer + offset, sizeof(int));
    std::cerr << "SSN: " << ssn << std::endl;

    std::cerr << "****Printing Buffer: End****" << std::endl << std::endl;
}

// Create an employee table
RC createTable(const std::string &tableName) {
    std::cerr << "****Create Table " << tableName << " ****" << std::endl;

    // 1. Create Table ** -- made separate now.
    std::vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength) 4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    attrs.push_back(attr);

    RC rc = rm.createTable(tableName, attrs);
    assert(rc == success);
    std::cerr << "****Table Created: " << tableName << " ****" << std::endl << std::endl;

    return success;
}

void prepareLargeTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int index, void *buffer,
                       int *size) {
    int offset = 0;

    // Null-indicators
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

    // Null-indicator for the fields
    memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = (char) (index % 26 + 97);

    for (unsigned i = 0; i < 10; i++) {
        // length
        memcpy((char *) buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        // varchar
        for (int j = 0; j < count; j++) {
            memcpy((char *) buffer + offset, &text, 1);
            offset += 1;
        }

        // integer
        memcpy((char *) buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // real
        auto real = (float) (index + 1);
        memcpy((char *) buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}

// Create a large table for pressure test
RC createLargeTable(const std::string &tableName) {
    std::cerr << "***** Creating a Large Table: " << tableName << " *****" << std::endl;

    // 1. Create Table ** -- made separate now.
    std::vector<Attribute> attrs;

    int index = 0;
    char *suffix = (char *) malloc(10);
    for (unsigned i = 0; i < 10; i++) {
        Attribute attr;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength) 4;
        attrs.push_back(attr);
        index++;
    }

    RC rc = rm.createTable(tableName, attrs);
    assert(rc == success);
    std::cerr << "***** A Large Table: " << tableName << " has created. *****" << std::endl << std::endl;

    free(suffix);

    return 0;
}

// Write RIDs to a disk - do not use this code.
// This is not a page-based operation. For test purpose only.
void writeRIDsToDisk(std::vector<RID> &rids) {
    remove("rids_file");
    std::ofstream ridsFile("rids_file", std::ios::out | std::ios::trunc | std::ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, std::ios::beg);
        for (auto &rid : rids) {
            ridsFile.write(reinterpret_cast<const char *>(&rid.pageNum),
                           sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char *>(&rid.slotNum),
                           sizeof(unsigned));
        }
        ridsFile.close();
    }
}

// Write sizes to a disk - do not use this code.
// This is not a page-based operation. For test purpose only.
void writeSizesToDisk(std::vector<int> &sizes) {
    remove("sizes_file");
    std::ofstream sizesFile("sizes_file", std::ios::out | std::ios::trunc | std::ios::binary);

    if (sizesFile.is_open()) {
        sizesFile.seekp(0, std::ios::beg);
        for (int &size : sizes) {
            sizesFile.write(reinterpret_cast<const char *>(&size),
                            sizeof(int));
        }
        sizesFile.close();
    }
}

// Read rids from the disk - do not use this code.
// This is not a page-based operation. For test purpose only.
void readRIDsFromDisk(std::vector<RID> &rids, int numRecords) {
    RID tempRID;
    unsigned pageNum;
    unsigned slotNum;

    std::ifstream ridsFile("rids_file", std::ios::in | std::ios::binary);
    if (ridsFile.is_open()) {
        ridsFile.seekg(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
            ridsFile.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFile.close();
    }
}

// Read sizes from the disk - do not use this code.
// This is not a page-based operation. For test purpose only.
void readSizesFromDisk(std::vector<int> &sizes, int numRecords) {
    int size;

    std::ifstream sizesFile("sizes_file", std::ios::in | std::ios::binary);
    if (sizesFile.is_open()) {

        sizesFile.seekg(0, std::ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFile.read(reinterpret_cast<char *>(&size), sizeof(int));
            sizes.push_back(size);
        }
        sizesFile.close();
    }
}
#endif

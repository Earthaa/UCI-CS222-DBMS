#include "rbfm.h"
using namespace std;
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
   return PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance().closeFile(fileHandle);
}
// convert record
// then use insertConvertedRecord to insert converted one
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    //convert record
    unsigned recordLength;
    char* convertedData = (char*)calloc(PAGE_SIZE, sizeof(char));

    DataHelper::rawToRecord(recordLength, convertedData, (char *) data, recordDescriptor);

    char* realRecord = (char*)malloc(recordLength);
    memcpy(realRecord, convertedData, recordLength);
    free(convertedData);
    RC rc = this->insertConvertedRecord(fileHandle, recordDescriptor, realRecord, recordLength, rid);
    free(realRecord);
    return rc;
}
// The record inserted here should be already converted
RC RecordBasedFileManager::insertConvertedRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const char *convertedRecord, const unsigned recordLength, RID &rid) {
    int pageNum = findAvailableExistedPage(fileHandle,recordLength);

    //append new page
    if (pageNum < 0){
        pageNum = fileHandle.getNumberOfPages();
        // char* newPagedata = (char*)calloc(PAGE_SIZE, sizeof(char));
        char* newPagedata = (char*)calloc(PAGE_SIZE, sizeof(char));
        DataHelper::getInitialRecordPage(convertedRecord, newPagedata, recordLength);

        rid.pageNum = pageNum;
        rid.slotNum = 0;
        RC rc = fileHandle.appendPage(newPagedata);
        free(newPagedata);
        return rc;
    //use found one
    } else{
        char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
        fileHandle.readPage(pageNum, pageData);
        RecordPage* recordPage = new RecordPage(pageNum, pageData);

        rid.slotNum = recordPage->getAvailableSlot(recordLength);
        rid.pageNum = pageNum;

        RC rc = recordPage->addNewRecord(rid.slotNum, recordLength, convertedRecord, fileHandle);

        free(pageData);
        delete recordPage;
        return rc;
    }
}


// The record get is the rawRecord not a converted one
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    if (rid.pageNum < 0 || rid.pageNum >= fileHandle.getNumberOfPages() || rid.slotNum < 0){
        return -1;
    }

    char* recordData = (char*)calloc(PAGE_SIZE, sizeof(char));
    unsigned recordLength = 0;

    RC rc;
    RID curRID = rid;
    do{
        rc = this->readConvertedRecord(fileHandle, curRID, recordLength, recordData);
        if(DataHelper::isPointerRecord(recordData))
            DataHelper::readRID(recordData, curRID);
        if(rc != 0) {
            free(recordData);
            return -1;
        }
    }while(DataHelper::isPointerRecord(recordData));

    if(rc < 0){
        free(recordData);
        return rc;
    }

    DataHelper::recordToRaw(recordData, (char *) data, recordDescriptor);

    free(recordData);
    return rc;
}

// Recursive delete
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {

    char* dataToDelete = (char*)calloc(PAGE_SIZE, sizeof(char));
    RID curRID;
    // length is useless in this method now
    unsigned length;
    curRID.slotNum = rid.slotNum;
    curRID.pageNum = rid.pageNum;
    RC rc = readConvertedRecord(fileHandle, curRID, length, dataToDelete);
        if(rc != 0){
            free(dataToDelete);
        return -1;
    }
    while(DataHelper::isPointerRecord(dataToDelete)){
        // do delete
        char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
        fileHandle.readPage(curRID.pageNum, pageData);
        RecordPage* recordPage = new RecordPage(curRID.pageNum, pageData);
        readConvertedRecord(fileHandle, curRID, length, dataToDelete);
        rc = recordPage->deleteRecord(curRID.slotNum, fileHandle);
        free(pageData);
        delete recordPage;
        if(rc != 0) {
            free(dataToDelete);
            return -1;
        }
        //update
        DataHelper::readRID(dataToDelete, curRID);
        readConvertedRecord(fileHandle, curRID, length, dataToDelete);
    }
    char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
    fileHandle.readPage(curRID.pageNum, pageData);

    RecordPage* recordPage = new RecordPage(curRID.pageNum, pageData);
    readConvertedRecord(fileHandle, curRID, length, dataToDelete);
    rc = recordPage->deleteRecord(curRID.slotNum, fileHandle);
    free(pageData);
    delete recordPage;
    free(dataToDelete);

    return rc;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int attributeNum = recordDescriptor.size();
    //store null value
    unsigned nullLength = DataHelper::getNullLength(recordDescriptor);
    char* nullByte = (char*)malloc(nullLength);
    // char nullByte[nullLength];
    memcpy(nullByte, data, nullLength);

    //calculate the converted dateLength
    unsigned pivot = nullLength;
    unsigned int stringLength;
    for (unsigned i = 0; i < attributeNum; i++){
        std::cout<<recordDescriptor.at(i).name<<": ";
        //check if null
        if (DataHelper::isNull(i, nullByte)){
            std::cout<<"NULL";
            std::cout<<"   ";
            continue;
        }
        char c[PAGE_SIZE];
        switch(recordDescriptor.at(i).type){
            case TypeVarChar:
                //read string length
                memcpy(&stringLength, (char*)data + pivot, sizeof(int));
                pivot += sizeof(int);
                //read string
                memcpy(c, (char*)data + pivot, stringLength);
                c[stringLength] = '\0';
                std::cout<< ( reinterpret_cast< char const* >(c));
                pivot += stringLength;
                break;
            case TypeInt:
                int intResult;
                memcpy(&intResult, (char*)data + pivot, sizeof(int));
                pivot += sizeof(int);
                std::cout<<intResult;
                break;
            case TypeReal:
                float floatResult;
                memcpy(&floatResult, (char*)data + pivot, sizeof(int));
                pivot += sizeof(int);
                cout<<std::fixed<<floatResult<<endl;
                break;
            default:
                std::cout<<"Error in DataHelper::calculateConvertedLength"<<std::endl<<std::endl<<std::endl;
                break;
        }
        std::cout<<"   ";
    }
    free(nullByte);
    std::cout<<std::endl;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    unsigned oldRecordLength;
    char* oldConvertedData = (char*)calloc(PAGE_SIZE, sizeof(char));
    this->readConvertedRecord(fileHandle, rid, oldRecordLength, oldConvertedData);
    //prevent pointer to pointer also perevent data to update here is not here
    bool isNotHere = false;

    RC rc = 0;
    //if its the pointerRecord, not the real one, the true data should be marked as not here if update
    RID curRID = rid;
    while (DataHelper::isPointerRecord(oldConvertedData)){
        isNotHere = true;
        rc = this->readConvertedRecord(fileHandle, curRID, oldRecordLength, oldConvertedData);
        if(rc != 0){
            free(oldConvertedData);
            return -1;
        }
         DataHelper::readRID(oldConvertedData, curRID);
    }
    free(oldConvertedData);

    //convert record
    unsigned newRecordLength;
    char* newConvertedData = (char*)calloc(PAGE_SIZE, sizeof(char));
    DataHelper::rawToRecord(newRecordLength, newConvertedData, (char *) data, recordDescriptor);

    //read old one
    char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
    fileHandle.readPage(rid.pageNum, pageData);
    RecordPage* recordPage = new RecordPage(rid.pageNum, pageData);
    free(pageData);
    if(isNotHere)
        DataHelper::setNotHereMark(newConvertedData);
    // When it does not exceed the limit of a page
    if(recordPage->getFreeBytes() >= newRecordLength - oldRecordLength){
        rc = recordPage->updateRecord(rid.slotNum, newRecordLength, newConvertedData, fileHandle);
    }
    else { //this page has no remaining space
        //insert into new place
        RID newRid;
        rc = this->insertConvertedRecord(fileHandle, recordDescriptor, newConvertedData, newRecordLength, newRid);
        if (rc < 0) {
            delete recordPage;
            free(newConvertedData);
            return rc;
        }
        //leave pointerRecord in the original place
        char* pointerRecord = (char*)calloc(PAGE_SIZE, sizeof(char));
        unsigned pointerRecordLength = 0;
        DataHelper::createPointerRecord(pointerRecord, pointerRecordLength, newRid);
        if(isNotHere)
            DataHelper::setNotHereMark(pointerRecord);
        rc = recordPage->updateRecord(rid.slotNum, pointerRecordLength, pointerRecord, fileHandle);
        free(pointerRecord);
    }
    delete recordPage;
    free(newConvertedData);
    return rc;
}

// It read an attribute without null bytes
RC RecordBasedFileManager::readConvertedAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                               const RID &rid, const std::string &attributeName, void *data,
                                               bool &isNull, int &attributeTrueLength) {
    if(rid.pageNum < 0 || rid.pageNum >= fileHandle.getNumberOfPages() || rid.slotNum < 0)
        return -1;
    isNull = false;
    // We use readRecord to get true data which prevents the pointer data
    char* rawData = (char*)calloc(PAGE_SIZE, sizeof(char));
    readRecord(fileHandle, recordDescriptor, rid, rawData);
    char* recordData = (char*)calloc(PAGE_SIZE, sizeof(char));
    unsigned length;
    DataHelper::rawToRecord(length, recordData, rawData, recordDescriptor);
    free(rawData);
    int indexPivot = 2 * sizeof(char) + sizeof(int);
    int recordPivot = 2 * sizeof(char) + sizeof(int) + recordDescriptor.size() * sizeof(int);
    RC rc = -1;
    for(int i = 0; i < recordDescriptor.size(); i++){
        int end;
        indexPivot = 2 * sizeof(char) + (i+1) * sizeof(int);
        memcpy(&end, recordData + indexPivot, sizeof(int));
        //find it
        if(recordDescriptor[i].name == attributeName){
            attributeTrueLength = end - recordPivot;
            if(end == recordPivot)
                isNull = true;
            //length should be added
            if(recordDescriptor[i].type == TypeVarChar && !isNull){
                // For we have \0 at the end
                attributeTrueLength -= 1;
                memcpy(data, &attributeTrueLength, sizeof(int));
                memcpy((char*)data + sizeof(int), recordData + recordPivot, attributeTrueLength);
                attributeTrueLength += sizeof(int);
            }
            else
                memcpy(data, recordData + recordPivot, attributeTrueLength);

            rc = 0;
            break;
        }
        recordPivot = end;
    }

    free(recordData);
    return rc;
}
//with null byte in front of the data
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
    bool isNull = false;
    RC rc;
    int attributeTrueLength;
    rc = readConvertedAttribute(fileHandle, recordDescriptor, rid, attributeName, rawAttribute, isNull,
                                attributeTrueLength);
    if(rc != 0){
        free(rawAttribute);
        return -1;
    }
    char nullBytes = 0x00;
    // 10000000
    if(isNull)
        nullBytes = 0x80;
    memcpy(data, &nullBytes, sizeof(char));

    memcpy((char*)data + sizeof(char), rawAttribute, attributeTrueLength);

    free(rawAttribute);
    return 0;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {

    return rbfm_ScanIterator.initScanIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

//Return the page num of the first available page
unsigned RecordBasedFileManager::findAvailableExistedPage(FileHandle &fileHandle, unsigned dataLength){
    int maxPage = fileHandle.getNumberOfPages();
    //when no page exist
    if (maxPage <= 0){
        return -1;
    }

    //check if last Page can contain
    char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
    fileHandle.readPage(maxPage - 1, pageData);
    RecordPage* recordPage = new RecordPage(maxPage - 1, pageData);
    if (recordPage->canContain(dataLength)){
        free(pageData);
        delete(recordPage);
        return maxPage - 1;
    }

    //search all the old page
    for (int i = 0; i < maxPage - 1; i++){
        delete recordPage;
        fileHandle.readPage(i, pageData);
        recordPage = new RecordPage(i, pageData);
        if(recordPage->canContain(dataLength)){
            delete recordPage;
            free(pageData);
            return i;
        }
    }
    delete recordPage;
    free(pageData);
    //no existed available
    return -1;
}

// read the converted record
RC RecordBasedFileManager::readConvertedRecord(FileHandle &fileHandle, const RID &rid, unsigned &convertedRecordLength,
                                               void *convertedRecordData) {
    if (rid.pageNum < 0 || rid.pageNum >= fileHandle.getNumberOfPages() || rid.slotNum < 0){
        return -1;
    }
    char* page = (char*)calloc(PAGE_SIZE, sizeof(char));
    fileHandle.readPage(rid.pageNum, page);
    RecordPage* recordPage = new RecordPage(rid.pageNum, page);
    //slot out of num
    convertedRecordLength = recordPage->getSlotData(rid.slotNum, convertedRecordData);
    free(page);
    delete(recordPage);

    if(convertedRecordLength == 0 || convertedRecordLength == -1){
        return -1;
    }
    return 0;
}


// given the position(ex. 3) and the nullByte(ex. [00100101,00000000...]), return if it's null or not
bool DataHelper::isNull(int index, char* nullByte){
    int bytePosition = index / 8;
    int bitPosition = index % 8;
    char b = nullByte[bytePosition];
    return ((b >> (7 - bitPosition)) & 0x1);
}

bool DataHelper::isPointerRecord(const char *convertedData){
    char mark;
    memcpy(&mark, convertedData, sizeof(char));
    return (mark & 0x01) == 0x01 ;
}

bool DataHelper::isRecordHere(const char *convertedData) {
    char mark;
    memcpy(&mark, convertedData, sizeof(char));
    return (mark & 0x02) != 0x02;
}

// This should be a pointer record
void DataHelper::readRID(const char *convertedData, RID& rid){
    //Assume: [mark, version, rid.page, rid.slot]
    unsigned page, slot;
    memcpy(&page, convertedData + 2 * sizeof(char), sizeof(unsigned));
    memcpy(&slot, convertedData + 2 * sizeof(char) + sizeof(unsigned), sizeof(unsigned));
    rid.pageNum = page;
    rid.slotNum = slot;
}
//TODO
void DataHelper::createPointerRecord(char* data, unsigned& dataLength, const RID& rid){
    unsigned page = rid.pageNum;
    unsigned slot = rid.slotNum;
    char mark = 0x1;
    char version = 0x0;

    memcpy(data, &mark , sizeof(char));
    memcpy(data + sizeof(char), &version , sizeof(char));
    memcpy(data + 2 * sizeof(char), &page , sizeof(unsigned));
    memcpy(data + 2 * sizeof(char) + sizeof(unsigned), &slot , sizeof(unsigned));

    dataLength = 10;
}

// convert the rareData into the convertedData, which is we store on disk. and also record the size of convertedData into recordLength
void DataHelper::rawToRecord(unsigned& recordLength, char *convertedData, const char *rawData, const std::vector<Attribute> &recordDescriptor){
    int attributeNum = recordDescriptor.size();
    //plus 1 to prevent overflow and calculate the last attribute's length
    unsigned indexLength = attributeNum * sizeof(unsigned);

    //store null value
    unsigned nullLength = DataHelper::getNullLength(recordDescriptor);

    char* nullByte = (char*)malloc(nullLength);
    // char nullByte[nullLength];
    memcpy(nullByte, rawData, nullLength);

    //insert mark and version by default
    // for mark, 0:pointer , 1:old place, 2:new place(do not return when scan)
    // char mark = 0x1, version = 0x0;
    unsigned markAndVersionLength =  2 * sizeof(char) + sizeof(unsigned);
    unsigned hiddenSpace = 0;
    char mark = 0x0, version = 0x0;
    memcpy(convertedData, &mark, sizeof(char));
    memcpy(convertedData + sizeof(char), &version, sizeof(char));
    memcpy(convertedData + 2 * sizeof(char), & hiddenSpace, sizeof(unsigned));

    //calculate the converted dateLength
    // convertedPivot is where to put data
    // indexPivot is where to put index
    unsigned dataLength = 0, rawPivot = nullLength, convertedPivot = indexLength + markAndVersionLength, indexPivot = markAndVersionLength;
    unsigned stringLength;
    for (unsigned i = 0; i < attributeNum; i++){
        indexPivot = markAndVersionLength + i * sizeof(unsigned);
        //if it's null, store the current convertedPivot but doesn't increment it.
        // in the page: next index minius current index equals 0 means current attribute is null
        if (DataHelper::isNull(i, nullByte)){
            memcpy(convertedData + indexPivot, &convertedPivot, sizeof(unsigned));
            continue;
        }
        char endOfString = '\0';
        switch(recordDescriptor.at(i).type){
            case TypeVarChar:
                //read string length
                memcpy(&stringLength, rawData + rawPivot, sizeof(int));
                //cout<<nullLength<<endl;
                dataLength += (unsigned) stringLength + sizeof(char);
                rawPivot += sizeof(int);
                //store the next attribute
                memcpy(convertedData + convertedPivot, rawData + rawPivot, stringLength);
                memcpy(convertedData + convertedPivot + stringLength, &endOfString, sizeof(char));
                rawPivot += (unsigned) stringLength;
                //store the end offset of current attribute
                convertedPivot += (unsigned) stringLength + sizeof(char);
                memcpy(convertedData + indexPivot, &convertedPivot, sizeof(unsigned));

                break;
            case TypeInt:
            case TypeReal:
                dataLength += sizeof(int);
                memcpy(convertedData + convertedPivot, rawData + rawPivot, sizeof(int));
                rawPivot += sizeof(int);
                convertedPivot += sizeof(int);
                memcpy(convertedData + indexPivot, &convertedPivot, sizeof(unsigned));
                break;
            default:
                std::cout<<"Error in DataHelper::calculateConvertedLength"<<std::endl<<std::endl<<std::endl;
                break;
        }
    }
    free(nullByte);
    unsigned totalLength = markAndVersionLength + indexLength + dataLength;
    recordLength = totalLength;
}

// convert the recordData(which is store inside the DataHelper instance) back to pageData.
// and also record the size of original pageData into dataLength
void DataHelper::recordToRaw(const char *convertedData, char *rawData, const std::vector<Attribute> &recordDescriptor) {
    // the size of the mark and version in the beginning of each record
    unsigned markAndVersionLength =  2 * sizeof(char) + sizeof(unsigned);

    int attributeNum = recordDescriptor.size();
    int nullLength = DataHelper::getNullLength(recordDescriptor);
    char n = 0x0, isNull = 0x1;
    int dataPivot = nullLength;
    unsigned int recordPosition,nextRecordPosition;
    recordPosition =sizeof(unsigned) * recordDescriptor.size() + markAndVersionLength;
    for (int i = 0; i < attributeNum; i++){
        memcpy(&nextRecordPosition, convertedData + i * sizeof(unsigned) + markAndVersionLength, sizeof(unsigned));

        int attributeLength = nextRecordPosition - recordPosition;
        //is null
        if (attributeLength == 0) n = n | (isNull << (7 - i % 8));
        if (i % 8 == 7){
            memcpy(rawData + (i / 8), &n, sizeof(char));
            n = 0x0;
        }

        if(attributeLength == 0) continue;
        //copy each attribute into pageData
        int rawAttributeLength;
        switch(recordDescriptor.at(i).type){
            case TypeVarChar:
                rawAttributeLength = attributeLength - sizeof(char);
                memcpy(rawData + dataPivot, &rawAttributeLength, sizeof(int));
                dataPivot += sizeof(int);
                //remove the last '\0' from the string
                memcpy(rawData + dataPivot, convertedData + recordPosition, rawAttributeLength);
                dataPivot += rawAttributeLength;
                break;
            case TypeInt:
            case TypeReal:
                memcpy(rawData + dataPivot, convertedData + recordPosition, sizeof(int));
                dataPivot += sizeof(int);
                break;
            default:
                std::cout<<"Error in DataHelper::recordToRaw"<<std::endl<<std::endl<<std::endl;
                break;
        }
        recordPosition = nextRecordPosition;
    }
    //put the last byte of null byte into pageData if it's total num is not multiple of 8s
    if (attributeNum % 8 != 0){
        memcpy(rawData + ((attributeNum - 1) / 8), &n, sizeof(char));
    }
}

// Generate a page with one record inside it, the record should be a converted one
void DataHelper::getInitialRecordPage(const char *record, char *pageData, unsigned recordLength) {
    unsigned dataBegin = PAGE_SIZE - recordLength;
    unsigned slotCount = 1;
    unsigned freeBytes = PAGE_SIZE - recordLength - 4 * sizeof(int);
    //[freeBytes, slotCount, dataBegin, dataLength, ...empty..., convertedData]
    memcpy(pageData, &freeBytes, sizeof(int));
    memcpy(pageData + sizeof(int), &slotCount, sizeof(int));
    memcpy(pageData + 2 * sizeof(int), &dataBegin, sizeof(int));
    memcpy(pageData + 3 * sizeof(int), &recordLength, sizeof(int));
    memcpy(pageData + dataBegin, record, recordLength);

}

//given the descriptor, calculate the length of the nullByte(ex. [01010000, 00000000], 9 attributes return 2 )
int DataHelper::getNullLength(const std::vector<Attribute> &recordDescriptor){
    int res = ceil((double)recordDescriptor.size() / 8);
    return res;
}

//mark this converted as new, so that scan will not read it.
void DataHelper::setNotHereMark(char * convertedData){
    char notHereMark = 0x02;
    char curMark;
    memcpy(&curMark, convertedData, sizeof(char));
    curMark |= notHereMark;
    memcpy(convertedData, &curMark, sizeof(char));
}

RecordPage::RecordPage(unsigned pageNum, const void *raw) {
    pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(pageData, raw, PAGE_SIZE);
    this->pageNum = pageNum;
    memcpy(&freeBytes, pageData, sizeof(int));
    memcpy(&totalSlot, pageData + sizeof(int), sizeof(int));

}

RecordPage::~RecordPage() {
    free(pageData);
}

// It return the dataLength, if invalid slotNum, return -1
unsigned RecordPage::getSlotData(const unsigned slotNum, void *data) {

//    cout<<pageNum<<"  "<<totalSlot<<endl;
    if(slotNum >= totalSlot) {
        return -1;
    }
    // Get the offset, freeSpace, slotCount
    unsigned offset = sizeof(int) + sizeof(int) + 2 * slotNum * sizeof(int);
    unsigned dataLength = 0, dataStart = 0;

    memcpy(&dataStart, pageData + offset, sizeof(int));
    memcpy(&dataLength, pageData + offset + sizeof(int), sizeof(int));

    if (dataStart == 0){
        return -1;
    }
    memcpy(data, pageData + dataStart , dataLength);

    return dataLength;
}

char* RecordPage::getPageData() {
    return this->pageData;
}

void RecordPage::getPageData(char *newData) {
    memcpy(newData, this->pageData, PAGE_SIZE);
}
// Here are two conditions with or without extending slot records
// In project one we only concern extending slot records
bool RecordPage::canContain(unsigned recordLength){
    return freeBytes >= recordLength + 2 * sizeof(int);
}

unsigned RecordPage::getAvailableSlot(const unsigned dataLength) {
    unsigned slotDataStart = 0;
    unsigned slotDataLength = 0;
    for(int i = 0; i < totalSlot; i++){
        memcpy(&slotDataStart, pageData + 2 * (1 + i) * sizeof(int), sizeof(int));
        memcpy(&slotDataLength, pageData + 2 * (1 + i) * sizeof(int) + sizeof(int), sizeof(int));
        if(slotDataStart == 0)
            return i;
    }
    return totalSlot;
}

RC RecordPage::addNewRecord(unsigned slotNum, const unsigned recordLength, const char *record, FileHandle& fileHandle) {
    if(slotNum > totalSlot)
        return -1;

    // update freeBytes, add new record and a new slot record
    unsigned dataEnd = PAGE_SIZE;
    //For all record are in the end of the page, we need to find the min start which should be
    //the end of new record
    unsigned curSlotStart = 0;
    for(unsigned i = 0; i < totalSlot; i++){
        memcpy(&curSlotStart, pageData + 2 * (1 + i) * sizeof(int), sizeof(int));
        if (curSlotStart != 0){
            dataEnd = std::min(curSlotStart, dataEnd);
        }
    }
    //Insert data
    unsigned dataStart = dataEnd - recordLength;
    memcpy(pageData + dataStart, record, recordLength);
    //Update slot
    memcpy(pageData + 2 * (slotNum + 1) * sizeof(int), &dataStart, sizeof(int));
    memcpy(pageData + 2 * (slotNum + 1) * sizeof(int) + sizeof(int), &recordLength, sizeof(int));

    //update totalSlot;
    if(slotNum == totalSlot){
        ++totalSlot;
        memcpy(pageData + sizeof(int), &totalSlot, sizeof(int));
        freeBytes = freeBytes - 2 * sizeof(int) - recordLength;
    } else {
        freeBytes = freeBytes - recordLength;
    }

    //update page header
    memcpy(pageData, &freeBytes, sizeof(int));

    RC rc = fileHandle.writePage(pageNum, pageData);

    return rc;
}

// This function is used to move all the data left the oldRecordLeftPoint delta bytes to right
// Remember this function do not persist data into the disk !!!
void RecordPage::compact(int oldRecordLeftPoint, int delta){
    unsigned slotDataStart = 0, leftMostStart = PAGE_SIZE;
    for(int i = 0; i < totalSlot; i++){
        memcpy(&slotDataStart, pageData + 2 * (1 + i) * sizeof(int), sizeof(int));
        //skip empty
        if (slotDataStart == 0) continue;

        //find the left most dataBegin
        if (slotDataStart < leftMostStart){
            leftMostStart = slotDataStart;
        }

        //update the slotStart if it is at the left of the compact point, including the updated one
        if (slotDataStart <= oldRecordLeftPoint){
            slotDataStart = slotDataStart + delta;
            memcpy(pageData + 2 * (1 + i) * sizeof(int), &slotDataStart, sizeof(int));
        }
    }
    //transfer the data, which is on the left to updated one, to the right, with delta distance.
    char* transferTemp = (char*)calloc(PAGE_SIZE, sizeof(char));
    unsigned transferLength = oldRecordLeftPoint - leftMostStart;
    memcpy(transferTemp, this->pageData + leftMostStart, transferLength);
    memcpy(this->pageData + leftMostStart + delta, transferTemp, transferLength);
    free(transferTemp);

    //update the freeBytes
    freeBytes += delta;
    memcpy(pageData, &freeBytes, sizeof(int));
}

RC RecordPage::deleteRecord(unsigned slotNum, FileHandle& fileHandle){
    unsigned offset = sizeof(int) + sizeof(int) + 2 * slotNum * sizeof(int);
    unsigned dataLength = 0, dataStart = 0;
    memcpy(&dataStart, pageData + offset, sizeof(int));
    memcpy(&dataLength, pageData + offset + sizeof(int), sizeof(int));

    this->compact(dataStart, dataLength);
    
    //the tombstone of slot, dataStart and dataLength are both 0
    unsigned tombstone = 0;
    memcpy(pageData + offset, &tombstone, sizeof(int));
    memcpy(pageData + offset + sizeof(int), &tombstone, sizeof(int));

    RC rc = fileHandle.writePage(this->pageNum, this->pageData);
    return rc;
}
// The delta length of updated record pass in this method, should not exceed the available space in this page
// If it do exceed, you should to preprocess to find page with enough space to add the record in
RC RecordPage::updateRecord(unsigned slotNum, const unsigned recordLength, const char *record, FileHandle &fileHandle) {
    if(slotNum > totalSlot)
        return -1;
    int oldRecordStart = 0;
    int oldRecordLength = 0;
    memcpy(&oldRecordStart, this->pageData + 2 * (1 + slotNum) * sizeof(int), sizeof(int));
    memcpy(&oldRecordLength, this->pageData + 2 * (1 + slotNum) * sizeof(int) + sizeof(int), sizeof(int));
    if(oldRecordStart == 0)
        return -1;

    int delta = oldRecordLength - recordLength;
    this->compact(oldRecordStart, delta);

    int newRecordStart = oldRecordStart + delta;
    memcpy(this->pageData + newRecordStart, record, recordLength);

    //int freeBytes = this->freeBytes + delta; we do have updated it in compact

    memcpy(this->pageData + 2 * (1 + slotNum) * sizeof(int) + sizeof(int), &recordLength, sizeof(int));
    memcpy(this->pageData + 2 * (1 + slotNum) * sizeof(int), &newRecordStart, sizeof(int));


    RC rc = fileHandle.writePage(this->pageNum, this->pageData);
    return rc;

}

int RecordPage::getFreeBytes() {
    return this->freeBytes;
}

int RecordPage::getTotalSlots() {
    return this->totalSlot;
}

RC RBFM_ScanIterator::initScanIterator(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const std::string &conditionAttribute, const CompOp &compOp, const void* value,
                                         const std::vector<std::string> &attributeNames) {
    PagedFileManager::instance().openFile(fileHandle.fileName,this->fileHandle);
    this->recordDescriptor = recordDescriptor;
    this->conditionAttribute = conditionAttribute;
    this->compOp = compOp;
    this->attributeNames = attributeNames;
    this->curPageNum = 0;
    this->curSlotNum = 0;
    this->value = (char*) calloc(PAGE_SIZE, sizeof(char));
    // value might not have the length of PAGE_SIZE
    if (compOp != NO_OP){
        int valueLength = 0;
        AttrType type;
        for(int i = 0; i < recordDescriptor.size(); i++){
            if(recordDescriptor[i].name == conditionAttribute){
                type = recordDescriptor[i].type;
                break;
            }
        }
        switch(type){
            case TypeReal:
                valueLength = 4;
                break;
            case TypeInt:
                valueLength = 4;
                break;
            case TypeVarChar:
                int charLength;
                memcpy(&charLength, value, sizeof(int));
                valueLength = sizeof(int) +charLength;
                break;
        }

        memcpy(this->value, value, valueLength);
    }
    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    if(!isRIDValid())
        return RBFM_EOF;
    AttrType conditionAttributeType;

    //Find conditionAttributeType first for future use
    for(int i = 0; i < this->recordDescriptor.size(); i++){
        if(this->recordDescriptor[i].name == this->conditionAttribute){
            conditionAttributeType = recordDescriptor[i].type;

            break;
        }
    }
    while(isRIDValid()) {
        // To test if this record here
        char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
        char* recordData = (char*)calloc(PAGE_SIZE, sizeof(char));
        this->fileHandle.readPage(curPageNum,pageData);
        RecordPage* recordPage = new RecordPage(curPageNum, pageData);
        //if it is deleted record, skip it
        if (recordPage->getSlotData(curSlotNum,recordData) == -1){
            free(pageData);
            free(recordData);
            delete(recordPage);
            increaseRID();
            continue;
            }
        //if the original record is not here, skip it
        if(!DataHelper::isRecordHere(recordData)) {
            free(pageData);
            free(recordData);
            delete(recordPage);
            increaseRID();
            continue;
        }
        //read from pointer record
        RID curRID;
        curRID.pageNum = curPageNum;
        curRID.slotNum = curSlotNum;
        unsigned length = 0;//serve no function here
        while (DataHelper::isPointerRecord(recordData)){
            DataHelper::readRID(recordData, curRID);
            RC rc = RecordBasedFileManager::readConvertedRecord(fileHandle, curRID, length, recordData);
            if(rc != 0){
                free(pageData);
                free(recordData);
                delete(recordPage);
                return -1;
            }
        }

        char* attributeData = (char*)calloc(PAGE_SIZE, sizeof(char));
        bool isNull = false;
        int attributeLength = 0;
        // RID curRID;
        // curRID.pageNum = curPageNum;
        // curRID.slotNum = curSlotNum;

        // The raw attributedData data without null mark in front of it
        // If it's varchar data, there will be a length descriptor at the beginning of it
        RecordBasedFileManager::instance().readConvertedAttribute(fileHandle, recordDescriptor, curRID,
                conditionAttribute,attributeData, isNull, attributeLength);
        bool isSatisfy = DataHelper::isCompareSatisfy(attributeData, this->value, compOp, conditionAttributeType);
        // Do not satisy the compOp
        if(!isSatisfy){
            free(pageData);
            free(recordData);
            increaseRID();
            free(attributeData);
            delete(recordPage);
            continue;
        }
        // Find it!
        else{
            rid.slotNum = curSlotNum;
            rid.pageNum = curPageNum;

            int newNullLength = ceil((double)attributeNames.size() / 8);
            char* rawData = (char* )calloc(PAGE_SIZE, sizeof(char));
            DataHelper::recordToRaw(recordData, rawData, recordDescriptor);
            int oldNullLength = DataHelper::getNullLength(recordDescriptor);
            char* newNullBytes = (char*)calloc(newNullLength, sizeof(char));
            char* oldNullBytes = (char*)calloc(oldNullLength, sizeof(char));
            memcpy(oldNullBytes, rawData, oldNullLength);
            int oldPivot = oldNullLength;
            int newPivot = newNullLength;

            for(int newIter = 0; newIter < attributeNames.size(); newIter++){
               oldPivot = oldNullLength;
               for(int oldIter = 0; oldIter < recordDescriptor.size(); oldIter++){
                   if(recordDescriptor[oldIter].name == attributeNames[newIter]){
                       if(!DataHelper::isNull(oldIter, oldNullBytes)){
                           int charLength = 0;
                           int a = 0;
                           float b = 0.0;
                           switch(recordDescriptor[oldIter].type){
                               case TypeVarChar:
                                   memcpy(&charLength, rawData + oldPivot, sizeof(int));
                                   memcpy((char*)data + newPivot, &charLength, sizeof(int));
                                   memcpy((char*)data + newPivot + sizeof(int), rawData + oldPivot + sizeof(int), charLength);
                                   newPivot = newPivot + sizeof(int) + charLength;
                                   break;
                               case TypeInt:
                               case TypeReal:
                                   memcpy((char*)data + newPivot, rawData + oldPivot, sizeof(int));
                                   newPivot = newPivot + sizeof(int);
                                   break;
                           }
                           break;
                       }
                       // attribute is null
                       else{
                           DataHelper::setNull(newIter, newNullBytes);
                           break;
                       }
                   }
                   // not the desired attribute, update oldIter
                   else {
                       if (!DataHelper::isNull(oldIter, oldNullBytes)) {
                           int charLength = 0;
                           switch (recordDescriptor[oldIter].type) {
                               case TypeVarChar:
                                   memcpy(&charLength, rawData + oldPivot, sizeof(int));
                                   oldPivot = oldPivot + sizeof(int) + charLength;
                                   break;
                               case TypeInt:
                               case TypeReal:
                                   oldPivot = oldPivot + sizeof(int);
                                   break;
                           }
                       }
                   }
               }
            }
            //set new null bytes to the start of data
            memcpy(data, newNullBytes, newNullLength);
            free(newNullBytes);
            free(oldNullBytes);
            free(pageData);
            free(recordData);
            free(rawData);
            free(attributeData);
            delete(recordPage);
            increaseRID();
            return 0;
        }
    }
    return RBFM_EOF;
}

void RBFM_ScanIterator::increaseRID() {
    if(curPageNum >= this->fileHandle.getNumberOfPages())
        return;
    char* pageData = (char*)calloc(PAGE_SIZE, sizeof(char));
    this->fileHandle.readPage(this->curPageNum, pageData);

    RecordPage* recordPage = new RecordPage(this->curPageNum, pageData);
    int totalSlots = recordPage->getTotalSlots();
    if(curSlotNum < totalSlots - 1)
        curSlotNum++;
    //The final slot in this page
    else {
        curSlotNum = 0;
        curPageNum = curPageNum + 1;
    }
    free(pageData);
    delete(recordPage);
}
bool RBFM_ScanIterator::isRIDValid() {
    return this->curPageNum < this->fileHandle.getNumberOfPages();
}

bool DataHelper::isCompareSatisfy(const void *arg1, const void *arg2, const CompOp &compOp, const AttrType &attrType) {
    if (compOp == NO_OP) return true;

    if(attrType == TypeInt){
        int firstOp;
        int secondOp;
        memcpy(&firstOp, arg1, sizeof(int));
        memcpy(&secondOp, arg2, sizeof(int));
        switch(compOp){
            case EQ_OP:
                return firstOp == secondOp;
            case LT_OP:
                return firstOp < secondOp;
            case LE_OP:
                return firstOp <= secondOp;
            case GT_OP:
                return firstOp > secondOp;
            case GE_OP:
                return firstOp >= secondOp;
            case NE_OP:
                return firstOp != secondOp;
            case NO_OP:
                return true;
            default:
                cout<<"something goes wrong!"<<endl;
                return false;
        }
    }
    else if(attrType == TypeReal){
        float firstOp;
        float secondOp;
        memcpy(&firstOp, arg1, sizeof(float));
        memcpy(&secondOp, arg2, sizeof(float));
        switch(compOp){
            case EQ_OP:
                return firstOp == secondOp;
            case LT_OP:
                return firstOp < secondOp;
            case LE_OP:
                return firstOp <= secondOp;
            case GT_OP:
                return firstOp > secondOp;
            case GE_OP:
                return firstOp >= secondOp;
            case NE_OP:
                return firstOp != secondOp;
            case NO_OP:
                return true;
            default:
                cout<<"something goes wrong!"<<endl;
                return false;
        }
    }
    else if(attrType == TypeVarChar){
        int firstOpLength = 0;
        int secondOpLength = 0;
        memcpy(&firstOpLength, arg1, sizeof(int));
        memcpy(&secondOpLength, arg2, sizeof(int));
        string first = string((char*)arg1 + sizeof(int), firstOpLength);
        string second = string((char*)arg2 + sizeof(int), secondOpLength);
        //cout<<first<<"  "<<second<<endl;
        if(compOp == EQ_OP) {
           if (firstOpLength != secondOpLength)
               return false;
           return std::memcmp(arg1, arg2, sizeof(int) + firstOpLength) == 0;
       }
       else if(compOp == NE_OP) {
           if (firstOpLength != secondOpLength)
               return true;
           return std::memcmp(arg1, arg2, sizeof(int) + firstOpLength) != 0;
       }
       else if(compOp == NO_OP) {
           return true;
       }
       else if(compOp == LT_OP){
            if(first == "" && second != "")
                return false;
            return first < second;
       }
       else if(compOp == LE_OP){
            if(first == "" && second != "")
                return false;
            return first <= second;
       }
       else if(compOp == GT_OP){
            return first > second;
       }
       else if(compOp == GE_OP){
            return first >= second;
       }
       else{
            cout<<"something goes wrong!"<<endl;
            return false;
        }
    }
    return false;
}

void DataHelper::setNull(int index, char *nullByte) {
    int byteNum = (double)index / 8;
    int bitNum = index % 8;
    char mask = 0x01 << (7 - bitNum);
    nullByte[byteNum] |= mask;
}
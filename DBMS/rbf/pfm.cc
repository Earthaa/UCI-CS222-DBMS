#include "pfm.h"
#include <cstdio>
#include <iostream>
PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    FILE* fp = fopen(fileName.c_str(), "r");
    if(fp) {
        fclose(fp);
        return -1;
    }
    fp = fopen(fileName.c_str(),"wb");

    //Create an hidden page which is used to save counters
    void* data = malloc(PAGE_SIZE);
    for(unsigned int i = 0; i < PAGE_SIZE; i++){
        *((unsigned char *)(data) + i) = 0;
    }
    rewind(fp);
    fwrite(data, sizeof(unsigned char), PAGE_SIZE,fp);
    free(data);
    fclose(fp);
    return 0;

}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if(remove(fileName.c_str()) != 0)
        return -1;
    return 0;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    FILE* fp = fopen(fileName.c_str(), "r+b");
    //file does not exist or this fileHandle has already been bound to an open file
    if(fp == NULL || fileHandle.fp != NULL){
        return -1;
    }
    fileHandle.fp = fp;
    unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
    fseek(fileHandle.fp, 0, SEEK_SET);

    //Read 4 bytes a time, and transform to unsigned
    //The order is readPageCounter, writePageCounter,appendPageCounter and pageNum
    for(int i = 0; i < 4; i++){
        fseek(fileHandle.fp, i * 4, SEEK_SET);
        fread(buffer, sizeof(unsigned char), sizeof(unsigned), fileHandle.fp);
        switch (i){
            case 0:
                FileHelper::bytesToUnsigned(buffer,fileHandle.readPageCounter);
                break;
            case 1:
                FileHelper::bytesToUnsigned(buffer,fileHandle.writePageCounter);
                break;
            case 2:
                FileHelper::bytesToUnsigned(buffer,fileHandle.appendPageCounter);
                break;
            case 3:
                FileHelper::bytesToUnsigned(buffer,fileHandle.pageNumCounter);
                break;
        }
    }
    free(buffer);
    fseek(fileHandle.fp, 0, SEEK_SET);
    fileHandle.fileName = fileName;
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if(!fileHandle.fp)
        return -1;

    fileHandle.refreshHiddenPage();
    fclose(fileHandle.fp);

    fileHandle.fp = NULL;
    return 0;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pageNumCounter = 0;
}

FileHandle::~FileHandle(){
    if(fp)
        fclose(fp);
    fp = NULL;
};

RC FileHandle::readPage(PageNum pageNum, void *data) {
    //pageNum do not exist

    if(!fp || pageNum >= pageNumCounter)
        return -1;
   
    fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
    if(fread(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;
    ++readPageCounter;
//    refreshHiddenPage();
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    //pageNum do not exist
    if(!fp || pageNum >= pageNumCounter)
        return -1;
    fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
    if(fwrite(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;
    ++writePageCounter;
//    refreshHiddenPage();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    if(!fp)
        return -1;
    if(fseek(fp,((pageNumCounter + 1) * PAGE_SIZE), SEEK_SET))
        return -1;

    if(fwrite(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;

    fseek(fp, 0, SEEK_SET);
    pageNumCounter++;
    appendPageCounter++;
//    refreshHiddenPage();
    return 0;

}

unsigned FileHandle::getNumberOfPages() {
    if(!fp)
        return -1;
    return pageNumCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    if(!fp)
        return -1;
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::refreshHiddenPage() {
    if(!fp)
        return -1;
    fseek(fp, 0, SEEK_SET);
    unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
    for(int i = 0; i < 4; i++){
        fseek(fp,4 * i, SEEK_SET);
        switch (i){
            case 0:
                FileHelper::unsignedToBytes(readPageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 1:
                FileHelper::unsignedToBytes(writePageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 2:
                FileHelper::unsignedToBytes(appendPageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 3:
                FileHelper::unsignedToBytes(pageNumCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
        }
    }
    fseek(fp, 0 , SEEK_SET);
    free(buffer);
    return 0;
}

void FileHelper::unsignedToBytes(const unsigned int n, unsigned char *bytes) {
    memcpy(bytes,&n, sizeof(unsigned));
}

void FileHelper::bytesToUnsigned(const unsigned char *bytes, unsigned &n) {
    memcpy(&n,bytes, sizeof(unsigned));
}


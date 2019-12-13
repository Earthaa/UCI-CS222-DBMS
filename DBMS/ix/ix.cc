#include "ix.h"
#include <queue>

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
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

RC IndexManager::destroyFile(const std::string &fileName) {
    if(remove(fileName.c_str()) != 0)
        return -1;

    return 0;
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    FILE* fp = fopen(fileName.c_str(), "r+b");
    //file does not exist or this fileHandle has already been bound to an open file
    if(fp == NULL || ixFileHandle.fp != NULL){
        return -1;
    }
    ixFileHandle.fp = fp;
    unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
    fseek(ixFileHandle.fp, 0, SEEK_SET);

    //Read 4 bytes a time, and transform to unsigned
    //The order is readPageCounter, writePageCounter,appendPageCounter and pageNum
    for(int i = 0; i < 5; i++){
        fseek(ixFileHandle.fp, i * 4, SEEK_SET);
        fread(buffer, sizeof(unsigned char), sizeof(unsigned), ixFileHandle.fp);
        switch (i){
            case 0:
                FileHelper::bytesToUnsigned(buffer,ixFileHandle.ixReadPageCounter);
                break;
            case 1:
                FileHelper::bytesToUnsigned(buffer,ixFileHandle.ixWritePageCounter);
                break;
            case 2:
                FileHelper::bytesToUnsigned(buffer,ixFileHandle.ixAppendPageCounter);
                break;
            case 3:
                FileHelper::bytesToUnsigned(buffer,ixFileHandle.pageNumCounter);
                break;
            case 4:
                FileHelper::bytesToUnsigned(buffer,ixFileHandle.root);
                break;
        }
    }
    free(buffer);
    fseek(ixFileHandle.fp, 0, SEEK_SET);
    ixFileHandle.fileName = fileName;
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    if(!ixFileHandle.fp)
        return -1;

    ixFileHandle.refreshHiddenPage();
    fclose(ixFileHandle.fp);

    ixFileHandle.fp = NULL;
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    int root = ixFileHandle.getRoot();
    if (root == -1){ // when there is no leaf and node in the tree
        char* leaf = (char*)calloc(PAGE_SIZE, sizeof(char));
        LeafHelper::createLeaf(leaf, IX_EOF, IX_EOF, IX_EOF);
        LeafHelper::insertToLeaf(leaf, attribute, (char*) key, rid);
        RC rc = 0;
        rc = ixFileHandle.appendPage(leaf);
        ixFileHandle.setRoot(++root);
        free(leaf);
        return rc;
    } else {
        return insertWithPageNum(ixFileHandle, attribute, key, rid, root);
    }
}

//givern the pageNum, recursively search though node until reach leaf, then insert in the leaf, check if split is needed
RC IndexManager::insertWithPageNum(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int pageNum){
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));
    ixFileHandle.readPage(pageNum, data);
    const char* charKey = (char*) key;

    if(LeafHelper::isLeaf(data)){
        RC rc = 0;
        //if there is still place after this insertion, then write to disk
        if (LeafHelper::haveSpace(data, attribute, charKey)){
            LeafHelper::insertToLeaf(data, attribute, charKey, rid);
            rc = ixFileHandle.writePage(pageNum, data);
            free(data);
            return rc;
        } else { //there is no more space, split is needed
            //create the new page
            int newPageID = ixFileHandle.getNumberOfPages();
            char* newPageData = (char*)calloc(PAGE_SIZE, sizeof(char));
            int next = LeafHelper::getNext(data);
            int parent = LeafHelper::getParent(data);
            LeafHelper::createLeaf(newPageData, parent, pageNum, next);
            LeafHelper::setNext(data, newPageID);

            LeafHelper::splitLeaf(data, newPageData);

            //needs to create new node here
            if (parent == -1){
                int newNodeID = newPageID + 1;
                char* newNodeData = (char*)calloc(PAGE_SIZE, sizeof(char));
                char* rightFirstKey = (char*)calloc(PAGE_SIZE, sizeof(char));

                LeafHelper::getFirstPair(newPageData, rightFirstKey);
                NodeHelper::createNodeWithKey(newNodeData, 1, -1, pageNum, rightFirstKey, newPageID, attribute);
                //update the parent from splited page
                LeafHelper::setParent(data, newNodeID);
                LeafHelper::setParent(newPageData, newNodeID);

                rc = ixFileHandle.writePage(pageNum, data);
                rc = ixFileHandle.appendPage(newPageData);
                rc = ixFileHandle.appendPage(newNodeData);
                ixFileHandle.setRoot(newNodeID);

                free(newNodeData);
                free(rightFirstKey);
                free(data);
                free(newPageData);
                return insertEntry(ixFileHandle, attribute, key, rid);

            } else {
                char* rightFirstKey = (char*)calloc(PAGE_SIZE, sizeof(char));
                LeafHelper::getFirstPair(newPageData, rightFirstKey);
                rc = ixFileHandle.writePage(pageNum, data);
                rc = ixFileHandle.appendPage(newPageData);
                rc = insertToNodeWithPage(ixFileHandle, parent, attribute, rightFirstKey, newPageID);
                free(rightFirstKey);
                free(data);
                free(newPageData);
                return insertEntry(ixFileHandle, attribute, key, rid);
            }
        }

    } else {
        int nextPage = NodeHelper::findPage(data, attribute, charKey);
        free(data);
        return insertWithPageNum(ixFileHandle, attribute, key, rid, nextPage);
    }

    free(data);
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    int root = ixFileHandle.getRoot();
    if (root == -1) {return -1;}
    return deleteWithPageNum(ixFileHandle, attribute, key, rid, root);
}

//givern the pageNum, recursively search though node until reach leaf, then delete in the leaf, check if merge is needed
RC IndexManager::deleteWithPageNum(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int pageNum){
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));
    ixFileHandle.readPage(pageNum, data);
    const char* charKey = (char*) key;

    if(LeafHelper::isLeaf(data)){
        RC rc = LeafHelper::deleteInLeaf(data, attribute, charKey, rid);
        if (rc < 0){
            free(data);
            return rc;
        }

        // if it is not the root and there is too many space inside, merge is needed
        if (ixFileHandle.getRoot() != pageNum && LeafHelper::needMerge(data, attribute)){
            //TODO merge, currently lazy delete
            rc = ixFileHandle.writePage(pageNum, data);
            free(data);
            return rc;
        } else { //no need to merge
            rc = ixFileHandle.writePage(pageNum, data);
            free(data);
            return rc;
        }

    } else {
        int nextPage = NodeHelper::findPage(data, attribute, charKey);
        free(data);
        return deleteWithPageNum(ixFileHandle, attribute, key, rid, nextPage);
    }
}

//givern the pageID, recursively insert to node and split if needed
RC IndexManager::insertToNodeWithPage(IXFileHandle &ixFileHandle, int nodePageID,const Attribute& attr, const char* key, int newPageID){
    RC rc = 0;
    char* nodeData = (char*)calloc(PAGE_SIZE, sizeof(char));
    rc = ixFileHandle.readPage(nodePageID, nodeData);

    //when there is no space in this node, split is needed
    if (!NodeHelper::haveSpace(nodeData, attr, key)){
        int parent = NodeHelper::getParent(nodeData);
        int layer = NodeHelper::getLayer(nodeData);
        int newNodeID = ixFileHandle.getNumberOfPages();
        char* newNodeData = (char*)calloc(PAGE_SIZE, sizeof(char));
        char* middleKey = (char*)calloc(PAGE_SIZE, sizeof(char));
        NodeHelper::createEmptyNode(newNodeData, layer, parent);
        NodeHelper::splitNode(nodeData, newNodeData, middleKey);

        //deside to insert left or right
        int diff = CommonHelper::compare(key, middleKey, attr);
        if (diff < 0){
            NodeHelper::insertKey(nodeData, attr, key, newPageID);
        } else {
            NodeHelper::insertKey(newNodeData, attr, key, newPageID);
        }
        updateParentInChildren(ixFileHandle, newNodeData, newNodeID);

        if (parent == -1){
            int newRootNodeID = newNodeID + 1;
            char* newRootNode = (char*)calloc(PAGE_SIZE, sizeof(char));
            NodeHelper::createNodeWithKey(newRootNode, layer + 1, IX_EOF, nodePageID, middleKey, newNodeID, attr);
            NodeHelper::setParent(nodeData, newRootNodeID);
            NodeHelper::setParent(newNodeData, newRootNodeID);

            rc = ixFileHandle.writePage(nodePageID, nodeData);
            rc = ixFileHandle.appendPage(newNodeData);
            rc = ixFileHandle.appendPage(newRootNode);

            free(nodeData);
            free(newNodeData);
            free(middleKey);
            free(newRootNode);
            ixFileHandle.setRoot(newRootNodeID);

            return rc;

        } else {
            rc = ixFileHandle.writePage(nodePageID, nodeData);
            rc = ixFileHandle.appendPage(newNodeData);
            free(nodeData);
            free(newNodeData);
            rc = insertToNodeWithPage(ixFileHandle, parent, attr, middleKey, newNodeID);
            free(middleKey);
            return rc;
        }
    } else {
        NodeHelper::insertKey(nodeData, attr, key, newPageID);
        rc = ixFileHandle.writePage(nodePageID, nodeData);
        free(nodeData);
        return rc;
    }
}

//update the parent value in the children of this page
RC IndexManager::updateParentInChildren(IXFileHandle &ixFileHandle, char* data, int pageID){
    RC rc = 0;
    int offsetPivot = 0, dataBegin = 0, count = NodeHelper::getCount(data), curPage = 0;
    char* child = (char*) calloc(PAGE_SIZE, sizeof(int));
    for(int i = 0; i < count; i++){
        offsetPivot = NodeHelper::headerSize + 2 * sizeof(int) * i;
        memcpy(&dataBegin, data + offsetPivot, sizeof(int));
        memcpy(&curPage, data + dataBegin - sizeof(int), sizeof(int));
        rc = ixFileHandle.readPage(curPage, child);
        LeafHelper::setParent(child, pageID);
        rc = ixFileHandle.writePage(curPage, child);
    }
    // the last pageID
    memcpy(&curPage, data + PAGE_SIZE - sizeof(int), sizeof(int));
    rc = ixFileHandle.readPage(curPage, child);
    LeafHelper::setParent(child, pageID);
    rc = ixFileHandle.writePage(curPage, child);

    free(child);
    return rc;
}


RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return ix_ScanIterator.initScanIterator(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    int root = ixFileHandle.getRoot();
    if (root == -1) {std::cout<<"ERROR: IndexManager::printBtree"<<std::endl<<std::endl<<std::endl;}
    NodeHelper::print(ixFileHandle, attribute, root, 0);
    std::cout<<std::endl;
}

//IX_ScanIterator::IX_ScanIterator() {
//}
//
//IX_ScanIterator::~IX_ScanIterator() {
//}

//RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
//    return -1;
//}
//
//RC IX_ScanIterator::close() {
//    return -1;
//}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    pageNumCounter = 0;
    root = 0;
}

IXFileHandle::~IXFileHandle(){
    if(fp)
        fclose(fp);
    fp = NULL;
};

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
    //pageNum do not exist

    if(!fp || pageNum >= pageNumCounter)
        return -1;

    fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
    if(fread(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;
    ++ixReadPageCounter;
//    refreshHiddenPage();
    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
    //pageNum do not exist
    if(!fp || pageNum >= pageNumCounter)
        return -1;
    fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
    if(fwrite(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;
    ++ixWritePageCounter;
//    refreshHiddenPage();
    return 0;
}

RC IXFileHandle::appendPage(const void *data) {
    if(!fp)
        return -1;
    if(fseek(fp,((pageNumCounter + 1) * PAGE_SIZE), SEEK_SET))
        return -1;

    if(fwrite(data, sizeof(unsigned char), PAGE_SIZE, fp) != PAGE_SIZE)
        return -1;

    fseek(fp, 0, SEEK_SET);
    pageNumCounter++;
    ixAppendPageCounter++;
//    refreshHiddenPage();
    return 0;

}

unsigned IXFileHandle::getNumberOfPages() {
    if(!fp)
        return -1;
    return pageNumCounter;
}

int IXFileHandle::getRoot(){
    return root - 1; // to avoid the unsigned to int problem
}

void IXFileHandle::setRoot(int newRoot){
    this->root = newRoot + 1;
}


RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    if(!fp)
        return -1;
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

RC IXFileHandle::refreshHiddenPage() {
    if(!fp)
        return -1;
    fseek(fp, 0, SEEK_SET);
    unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
    for(int i = 0; i < 5; i++){
        fseek(fp,4 * i, SEEK_SET);
        switch (i){
            case 0:
                FileHelper::unsignedToBytes(ixReadPageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 1:
                FileHelper::unsignedToBytes(ixWritePageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 2:
                FileHelper::unsignedToBytes(ixAppendPageCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 3:
                FileHelper::unsignedToBytes(pageNumCounter, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
            case 4:
                FileHelper::unsignedToBytes(root, buffer);
                fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fp);
        }
    }
    fseek(fp, 0 , SEEK_SET);
    free(buffer);
    return 0;
}

bool CommonHelper::isLeaf(char* data){
    int layer = 0;
    int pivot = 0; //the first item in leaf
    memcpy(&layer, data + pivot, sizeof(int));
    return layer == 0;
}

int CommonHelper::getFreeSpace(char* data){
    int freeSpace = 0;
    int pivot = sizeof(int); //skip layer
    memcpy(&freeSpace, data + pivot, sizeof(int));
    return freeSpace;
}

int CommonHelper::getCount(char* data){
    int count = 0;
    int pivot = 2 * sizeof(int); //skip layer and freeSpace
    memcpy(&count, data + pivot, sizeof(int));
    return count;
}


int CommonHelper::getParent(char* data){
    int parent = 0;
    int pivot = 3 * sizeof(int); //skip layer, freeSpace and count
    memcpy(&parent, data + pivot, sizeof(int));
    return parent;
}

void CommonHelper::setParent(char* data, int pageID){
    int pivot = 3 * sizeof(int); //skip layer, freeSpace and count
    memcpy(data + pivot, &pageID, sizeof(int));
}

int LeafHelper::getPrevious(char* data){
    int previous = 0;
    int pivot = 4 * sizeof(int); //skip layer, freeSpace, count and parent
    memcpy(&previous, data + pivot, sizeof(int));
    return previous;
}

int LeafHelper::getNext(char* data){
    int next = 0;
    int pivot = 5 * sizeof(int); //skip layer, freeSpace, count, parent and previous
    memcpy(&next, data + pivot, sizeof(int));
    return next;
}


void LeafHelper::setNext(char* data, int pageID){
    int pivot = 5 * sizeof(int); //skip layer, freeSpace, count, parent and previous
    memcpy(data + pivot, &pageID, sizeof(int));
}

//check if this leaf still has space for another key
bool LeafHelper::haveSpace(char* data, const Attribute& attr, const char* key){
    int freeSpace = getFreeSpace(data);
    int attrLength = 0;
    switch(attr.type){
        case TypeInt:
        case TypeReal:
            attrLength += sizeof(int);
            break;
        case TypeVarChar:
            memcpy(&attrLength, key, sizeof(int));
            attrLength += sizeof(int);
            break;
    }
    return freeSpace > (attrLength + 4 * sizeof(int)); // the freeSpace must bigger than the attrLength, offset(8 bytes) and RID(8 bytes)
}

//check if there is too many space in the leaf
bool LeafHelper::needMerge(char* data, const Attribute& attr){
    //TODO this function needs improvement
    int freeSpace = getFreeSpace(data);
    int totalSpace = PAGE_SIZE - LeafHelper::headerSize;
    return totalSpace < 2 * freeSpace;
}

//compare the key, return the difference between the key (not include the RID)
float CommonHelper::compare(const char* newKey, const char* oldKey, const Attribute& attr){
    int newInt = 0,oldInt = 0;
    float newFloat = 0,oldFloat = 0;
    int newLength = 0,oldLength = 0;
    switch(attr.type){
        case TypeInt:
            memcpy(&newInt, newKey, sizeof(int));
            memcpy(&oldInt, oldKey, sizeof(int));
            return newInt - oldInt;
        case TypeReal:
            memcpy(&newFloat, newKey, sizeof(int));
            memcpy(&oldFloat, oldKey, sizeof(int));
            return newFloat - oldFloat;
        case TypeVarChar:
            memcpy(&newLength, newKey, sizeof(int));
            memcpy(&oldLength, oldKey, sizeof(int));

            char* newChar = (char*)calloc(PAGE_SIZE, sizeof(char));
            char* oldChar = (char*)calloc(PAGE_SIZE, sizeof(char));

            memcpy(newChar, newKey + sizeof(int), newLength);
            memcpy(oldChar, oldKey + sizeof(int), oldLength);
            newChar[newLength] = '\0';
            oldChar[oldLength] = '\0';
            float res = strcmp(newChar, oldChar);

            free(newChar);
            free(oldChar);
            return res;
    }
}

//create an empty leaf with parent and next
//assuming the data is all 0
void LeafHelper::createLeaf(char* data, int parent, int previous, int next){
    int freeSpace = PAGE_SIZE - LeafHelper::headerSize;
    int pivot = 1 * sizeof(int); // the freeSpace, skip layer
    memcpy(data + pivot, &freeSpace, sizeof(int));

    pivot = 3 * sizeof(int); //the parent position, skip layer, freeSpace and count
    memcpy(data + pivot, &parent, sizeof(int));

    pivot = 4 * sizeof(int); //the next position, skip layer, freeSpace, count and parent
    memcpy(data + pivot, &previous, sizeof(int));

    pivot = 5 * sizeof(int); //the next position, skip layer, freeSpace, count, parent and previous
    memcpy(data + pivot, &next, sizeof(int));
}

//insert the key in to the right position of this leaf
//the record order is small to large from left to right
void LeafHelper::insertToLeaf(char* leafData, const Attribute& attr, const char* key, const RID& rid){
    int offsetIndex = 0, originalRecordBegin = PAGE_SIZE, leftMostRecord = PAGE_SIZE;
    findKeyPosition(leafData, attr, key, offsetIndex, originalRecordBegin, leftMostRecord, false);

    int count = getCount(leafData), delta = 2 * sizeof(int);//there are always RID (8 bytes) there
    //calculate delta
    if (attr.type == TypeVarChar){
        int strLength;
        memcpy(&strLength, key, sizeof(int));
        delta += strLength + sizeof(int);
    } else {
        delta += sizeof(int);
    }

    int dataBegin = originalRecordBegin - delta; // originalRecordBegin is the right position to insert
    compact(leafData, leftMostRecord, originalRecordBegin, -delta); //negative delta to indicate moving to left
    insertOffset(leafData, offsetIndex, dataBegin, delta);

    //insert the key:rid pair
    memcpy(leafData + dataBegin, key, delta - 2 * sizeof(int));
    int pageNum = rid.pageNum, slotNum = rid.slotNum;
    memcpy(leafData + originalRecordBegin - 2 * sizeof(int), &pageNum, sizeof(int));
    memcpy(leafData + originalRecordBegin - 1 * sizeof(int), &slotNum, sizeof(int));

    setCountAndSpace(leafData, 1, delta + 2 * sizeof(int));//occupied space is delta(record length) and the offsetLength(8 byte)
}

RC LeafHelper::deleteInLeaf(char* leafData, const Attribute& attr, const char* key, const RID& rid){
    int offsetIndex = 0;
    int originalRecordBegin = PAGE_SIZE, leftMostRecord = PAGE_SIZE; // no function here
    float diff = findKeyPosition(leafData, attr, key, offsetIndex, originalRecordBegin, leftMostRecord, true);
    if (diff != 0) {return -1;} // not found

    int count = getCount(leafData);
    int delta, dataLengthPivot =  LeafHelper::headerSize + 2 * offsetIndex * sizeof(int) + sizeof(int);
    memcpy(&delta, leafData + dataLengthPivot , sizeof(int));
    compact(leafData,leftMostRecord, originalRecordBegin, delta); // use compact to delte the entry
    deleteOffset(leafData, offsetIndex, delta);

    setCountAndSpace(leafData, -1, -delta - 2 * sizeof(int));
    return 0;
}

//givern the key and leafPage, find the suitable key position
//currently use linear search
//return with the difference between newKey and oldKey when found (for deletion)
float CommonHelper::findKeyPosition(char* leafData, const Attribute& attr, const char* key, int& offsetIndex, int& recordBegin, int& leftMostRecord, int headerSize, bool inclusive){
    int offsetPivot = headerSize;
    int count = getCount(leafData);
    int recordLength;
    float diff = 0;
    leftMostRecord = PAGE_SIZE;
    char* oldKey = (char*)calloc(PAGE_SIZE, sizeof(char));

    //go through the offset
    for (offsetIndex = 0; offsetIndex < count; offsetIndex++){
        offsetPivot = headerSize + offsetIndex * 2 * sizeof(int);
        memcpy(&recordBegin, leafData + offsetPivot, sizeof(int));
        memcpy(&recordLength, leafData + offsetPivot + sizeof(int), sizeof(int));
        if (offsetIndex == 0){ leftMostRecord = recordBegin;} //assuming the record is small to large from left to right

        memcpy(oldKey, leafData + recordBegin, recordLength);//doesn't remove RID at the back, seems no problem here
        if ((diff = compare(key,oldKey, attr)) < 0){
            free(oldKey);
            return diff; // found
        } else if (diff == 0){
            //TODO compare RID, extra
            if(inclusive) {
                free(oldKey);
                return 0;
            }
            continue;
        }
    }

    //not found
    recordBegin = PAGE_SIZE;
    free(oldKey);
    return 100;
}

//shift the record between leftEndPoint and rightEndPoint with delta
//when delta is larger than 0, moving the record to the right
void CommonHelper::compact(char* leafData, int leftEndPoint, int rightEndPoint, int delta){
    char* temp = (char*) calloc(PAGE_SIZE, sizeof(char));
    //copy the data to the right with delta
    memcpy(temp, leafData + leftEndPoint, rightEndPoint - leftEndPoint);
    memcpy(leafData + leftEndPoint + delta, temp, rightEndPoint - leftEndPoint);

    free(temp);
}

//insert the new offset into the offsetIndex position
//reduce the offset that is smaller than offsetIndex position with dataLength
void CommonHelper::insertOffset(char* leafData, int offsetIndex, int newDataBegin, int newDataLength, int headerSize){
    // minus newDataLength in dataBegin of offset which is before offsetIndex
    changeOffsetValue(leafData, offsetIndex, -newDataLength, headerSize);

    int count = getCount(leafData);
    //copy offset that is larger or equal to offsetIndex to the right
    int offsetEnd = headerSize + count * 2 * sizeof(int);
    int offsetPivot = headerSize + offsetIndex * 2 * sizeof(int);
    compact(leafData, offsetPivot, offsetEnd, 2 * sizeof(int));

    //insert the newDataBegin and newDataLength into the leafData
    memcpy(leafData + offsetPivot, &newDataBegin, sizeof(int));
    memcpy(leafData + offsetPivot + sizeof(int), &newDataLength, sizeof(int));
}

//delete the offset at offsetIndex position
//add the offset that is smaller than offsetIndex with delta
void CommonHelper::deleteOffset(char* leafData, int offsetIndex, int delta, int headerSize){
    // add delta in dataBegin of offset which is before offsetIndex
    changeOffsetValue(leafData, offsetIndex, delta, headerSize);

    int count = getCount(leafData);
    char* temp = (char*) calloc(PAGE_SIZE, sizeof(char));
    //copy offset that is larger or equal to offsetIndex to the right
    int offsetEnd = headerSize + count * 2 * sizeof(int);
    int offsetPivot = headerSize + offsetIndex * 2 * sizeof(int) + 2 * sizeof(int);//get the new offset position to compact
    int offsetCompactSize = 2 * sizeof(int);
    compact(leafData, offsetPivot, offsetEnd, -offsetCompactSize);
    free(temp);
}

//change the offset that is smaller than offsetIndex position with delta
//delta > 0 means to the right
void CommonHelper::changeOffsetValue(char* leafData, int offsetIndex, int delta, int headerSize){
    int dataBegin;
    for (int i = 0; i < offsetIndex; i++){
        int offsetPivot = headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, leafData + offsetPivot, sizeof(int));
        dataBegin += delta;
        memcpy(leafData + offsetPivot, &dataBegin, sizeof(int));
    }
}

//split half of the data in originalPage to the newPage
void LeafHelper::splitLeaf(char* originalPage, char* newPage){
    //get the start point of offset and record
    int count = getCount(originalPage);
    int splitOffsetIndex = count / 2; // which is better, split according to offset index or space? currently using index
    int splitOffsetPivot = LeafHelper::headerSize + 2 * sizeof(int) * splitOffsetIndex;
    int splitRecordBegin = 0;
    memcpy(&splitRecordBegin, originalPage + splitOffsetPivot, sizeof(int));
    int transferRecordSize = PAGE_SIZE - splitRecordBegin;
    int transferOffsetCount = count - splitOffsetIndex;

    //transfer record and offset
    memcpy(newPage + splitRecordBegin, originalPage + splitRecordBegin, transferRecordSize);
    memcpy(newPage + LeafHelper::headerSize, originalPage + splitOffsetPivot, transferOffsetCount * 2 * sizeof(int));

    //set Count and Memory
    LeafHelper::setCountAndSpace(originalPage, -transferOffsetCount, -transferRecordSize - transferOffsetCount * 2 * sizeof(int));
    LeafHelper::setCountAndSpace(newPage, transferOffsetCount, transferRecordSize + transferOffsetCount * 2 * sizeof(int));

    //compact the original data
    LeafHelper::compact(originalPage, LeafHelper::findLeftMostPoint(originalPage), splitRecordBegin, transferRecordSize);

    //update the offset position
    LeafHelper::changeOffsetValue(originalPage, splitOffsetIndex, transferRecordSize);
}

//get the first key:rid pair in this leaf
void CommonHelper::getFirstPair(char* data, char* key, int headerSize){
    getPair(data, key, 0, headerSize);
}

//get the pair at index
void CommonHelper::getPair(char* data, char* key, int index, int headerSize){
    int pivot = headerSize + 2 * sizeof(int) * index;
    int dataBegin = 0, dataLength = 0;
    memcpy(&dataBegin, data + pivot, sizeof(int));
    memcpy(&dataLength, data + pivot + sizeof(int), sizeof(int));
    memcpy(key, data + dataBegin, dataLength);
}

void CommonHelper::getKeyAndRIDFromPair(char* pair, const Attribute& attr, RID& rid, void* key){
    int strLength = 0, pageNum = 0, slotNum = 0;
    switch(attr.type){
        case TypeVarChar:
            memcpy(&strLength, pair, sizeof(int));
            memcpy(key, pair, strLength + sizeof(int));
            memcpy(&pageNum, pair + strLength + sizeof(int), sizeof(int));
            memcpy(&slotNum, pair + strLength + 2 * sizeof(int), sizeof(int));
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;
            break;
        case TypeInt:
        case TypeReal:
            memcpy(key, pair, sizeof(int));
            memcpy(&pageNum, pair + sizeof(int), sizeof(int));
            memcpy(&slotNum, pair + 2 * sizeof(int), sizeof(int));
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;
            break;
    }
}

void LeafHelper::print(char* data, const Attribute &attribute){
    std::cout<<"{\"keys\": [";

    int count = getCount(data), offsetPivot = 0;
    int dataBegin = 0,dataLength = 0;
    char* key = (char*) calloc(PAGE_SIZE, sizeof(char));
    int pageNum = 0, slotNum = 0;
    int intKey = 0, strLength = 0;
    float floatKey = 0;
    char* stringKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    for (int i = 0; i < count; i++){
        offsetPivot = LeafHelper::headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, data + offsetPivot, sizeof(int));
        memcpy(&dataLength, data + offsetPivot + sizeof(int), sizeof(int));

        memcpy(key, data + dataBegin, dataLength - 2 * sizeof(int));
        memcpy(&pageNum, data + dataBegin + dataLength - 2 * sizeof(int), sizeof(int));
        memcpy(&slotNum, data + dataBegin + dataLength - 1 * sizeof(int), sizeof(int));

        std::cout<< "\"";
        switch(attribute.type){
            case TypeVarChar:
                memcpy(&strLength, key, sizeof(int));
                memcpy(stringKey, key + sizeof(int), strLength);
                stringKey[strLength] = '\0';
                std::cout<<( reinterpret_cast< char const* >(stringKey));
                break;
            case TypeInt:
                memcpy(&intKey, key, sizeof(int));
                std::cout<<intKey;
                break;
            case TypeReal:
                memcpy(&floatKey, key, sizeof(int));
                std::cout<<floatKey;
                break;
        }
        std::cout<<":[("<<pageNum<<","<<slotNum<<")]\"";
        if (i != count - 1) {std::cout<<",";}
    }
    free(key);
    free(stringKey);
    std::cout<<"]}";
}

//set the count and space in the leafPage
void CommonHelper::setCountAndSpace(char* data, int countDelta, int spaceDelta){
    int count = getCount(data);
    int freeSpace = getFreeSpace(data);

    int pivot = sizeof(int);
    freeSpace -= spaceDelta;
    memcpy(data + pivot, &freeSpace, sizeof(int));

    pivot = 2 * sizeof(int);
    count += countDelta;
    memcpy(data + pivot, &count, sizeof(int));
}

int CommonHelper::findLeftMostPoint(char* data, int headerSize){
    if (getCount(data) == 0) return PAGE_SIZE;
    int point = 0;
    memcpy(&point, data + headerSize, sizeof(int)); // assuming data in the leftmost offset is also leftmost
    return point;
}


void NodeHelper::print(IXFileHandle &ixFileHandle, const Attribute &attribute, int position, int depth){
    char* data = (char*) calloc(PAGE_SIZE, sizeof(char));
    ixFileHandle.readPage(position, data);
    if (!isNode(data)){
        LeafHelper::print(data, attribute);
        free(data);
        return;
    }

    std::cout<<"{\"keys\": [";

    std::queue<int> children;
    int child = 0;
    int count = getCount(data), offsetPivot = 0;
    int dataBegin = 0,dataLength = 0;
    char* key = (char*) calloc(PAGE_SIZE, sizeof(char));
    int intKey = 0, strLength = 0;
    float floatKey = 0;
    char* stringKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    //keys
    for (int i = 0; i < count; i++){
        offsetPivot = NodeHelper::headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, data + offsetPivot, sizeof(int));
        memcpy(&dataLength, data + offsetPivot + sizeof(int), sizeof(int));
        memcpy(key, data + dataBegin, dataLength);
        memcpy(&child, data + dataBegin - sizeof(int), sizeof(int));
        children.push(child);

        std::cout<< "\"";
        switch(attribute.type){
            case TypeVarChar:
                memcpy(&strLength, key, sizeof(int));
                memcpy(stringKey, key + sizeof(int), strLength);
                stringKey[strLength] = '\0';
                std::cout<<( reinterpret_cast< char const* >(stringKey));
                break;
            case TypeInt:
                memcpy(&intKey, key, sizeof(int));
                std::cout<<intKey;
                break;
            case TypeReal:
                memcpy(&floatKey, key, sizeof(int));
                std::cout<<floatKey;
                break;
        }
        std::cout<<"\"";
        if (i != count - 1) {std::cout<<",";}
    }
    memcpy(&child, data + PAGE_SIZE - sizeof(int), sizeof(int));
    children.push(child);

    free(data);
    free(key);
    free(stringKey);
    std::cout<<"],"<<std::endl;

    //children
    std::cout << std::string(depth * 2, ' ');
    std::cout<<"\"children\": ["<<std::endl;
    while(!children.empty()){
        std::cout << std::string(depth * 2, ' ');
        std::cout<<"    ";
        NodeHelper::print(ixFileHandle, attribute, children.front(), depth + 4);
        children.pop();
        if (!children.empty()){ std::cout<<",";}
        std::cout<<std::endl;
    }
    std::cout << std::string(depth * 2, ' ');
    std::cout<<"]}";
}

int NodeHelper::getLayer(char* data){
    int layer = 0;
    int pivot = 0; //the first item in leaf
    memcpy(&layer, data + pivot, sizeof(int));
    return layer;
}

//create a node, with layer,parent inside
//also insert two pageID and one key
void NodeHelper::createNodeWithKey(char* data, int layer, int parent, int leftPageID, char* firstKey, int rightPageID, Attribute attr){
    createEmptyNode(data, layer, parent);

    int keyLength = 2 * sizeof(int);//including the RID for extra credit
    if (attr.type == TypeVarChar){
        int strLength = 0;
        memcpy(&strLength, firstKey, sizeof(int));
        keyLength += sizeof(int) + strLength;
    } else {
        keyLength += sizeof(int);
    }

    //store the rightPageID
    int pivot = PAGE_SIZE - sizeof(int);
    memcpy(data + pivot, &rightPageID, sizeof(int));

    //store the first key
    pivot -= keyLength;
    memcpy(data + pivot, firstKey, keyLength);

    //store the leftPageID
    pivot -= sizeof(int);
    memcpy(data + pivot, &leftPageID, sizeof(int));

    //create offset
    int dataBegin = PAGE_SIZE - sizeof(int) - keyLength; //point to the key
    int dataLength = keyLength; // doesn't contain any pageID
    int offsetPivot = NodeHelper::headerSize;
    memcpy(data + offsetPivot, &dataBegin, sizeof(int));
    offsetPivot += sizeof(int);
    memcpy(data + offsetPivot, &dataLength, sizeof(int));

    NodeHelper::setCountAndSpace(data, 1, NodeHelper::headerSize + keyLength + 4 * sizeof(int));// header, offset(8 bytes), 2 * pageID, key
}

//create a node with only layer and parent data
void NodeHelper::createEmptyNode(char* data, int layer, int parent){
    int freeSpace = PAGE_SIZE - NodeHelper::headerSize;
    int pivot = 0; // the layer, first item
    memcpy(data + pivot, &layer, sizeof(int));

    pivot = 1 * sizeof(int); // the freeSpace, skip layer
    memcpy(data + pivot, &freeSpace, sizeof(int));

    pivot = 3 * sizeof(int); //the parent position, skip layer, freeSpace and count
    memcpy(data + pivot, &parent, sizeof(int));
}

//find the pageID with key, return the pageID on the right of the key
int NodeHelper::findPage(char* data, const Attribute& attr, const char* key){
    int offsetIndex = 0, recordBegin = 0, leftMostRecord = 0;
    NodeHelper::findKeyPosition(data, attr, key, offsetIndex, recordBegin, leftMostRecord, false);
    int pagePosition = 0;
    memcpy(&pagePosition, data + recordBegin - sizeof(int), sizeof(int));
    return pagePosition;
}

//get the first pageID from the pageData
int NodeHelper::getFirstPage(char* data){
    int offsetPivot = NodeHelper::headerSize;
    int dataBegin = 0;
    memcpy(&dataBegin, data + offsetPivot, sizeof(int));
    int pageID = 0;
    memcpy(&pageID, data + dataBegin - sizeof(int), sizeof(int));
    return pageID;
}

// insert key and one pageID following it into the nodeData
void NodeHelper::insertKey(char* data, const Attribute& attr, const char* key, int pageID){
    int offsetIndex = 0, originalRecordBegin = 0, leftMostRecord = 0;
    NodeHelper::findKeyPosition(data, attr, key, offsetIndex, originalRecordBegin, leftMostRecord, false);
    leftMostRecord -= sizeof(int); // there is one more PageID

    int count = getCount(data), delta = 3 * sizeof(int);//there are RID, one PageID following the key
    //calculate delta
    if (attr.type == TypeVarChar){
        int strLength;
        memcpy(&strLength, key, sizeof(int));
        delta += strLength + sizeof(int);
    } else {
        delta += sizeof(int);
    }

    int dataBegin = originalRecordBegin - delta; // originalRecordBegin is the right position to insert
    compact(data, leftMostRecord, originalRecordBegin, -delta); //negative delta to indicate moving to left
    insertOffset(data, offsetIndex, dataBegin, delta);

    //insert the key and the pointer after it
    memcpy(data + dataBegin, key, delta - sizeof(int)); // remove the length of the pageID
    memcpy(data + dataBegin + delta - sizeof(int), &pageID, sizeof(int));

    setCountAndSpace(data, 1, delta + 2 * sizeof(int));//occupied space is delta(record length) and the offsetLength(8 byte)
}

bool NodeHelper::haveSpace(char* data, const Attribute& attr, const char* key){
    int freeSpace = getFreeSpace(data);
    int attrLength = 0;
    switch(attr.type){
        case TypeInt:
        case TypeReal:
            attrLength += sizeof(int);
            break;
        case TypeVarChar:
            memcpy(&attrLength, key, sizeof(int));
            attrLength += sizeof(int);
            break;
    }
    attrLength += 20;//test, i don;t know why it goes wrong if you delete this line
    return freeSpace > (attrLength + 3 * sizeof(int)); // the freeSpace must bigger than the attrLength, offset(8 bytes) and one page ID
}

//split the half of the info in originalPage into new page
// the middle key in originalPage will be extracted out
void NodeHelper::splitNode(char* originalPage, char* newPage, char* middleKey){
    //get the start point of offset and record
    int count = getCount(originalPage);
    int leftSplitOffsetIndex = count / 2; // which is better, split according to offset index or space? currently using index
    int rightSplitOffsetIndex = leftSplitOffsetIndex + 1;
    int leftSplitOffsetPivot = NodeHelper::headerSize + 2 * sizeof(int) * leftSplitOffsetIndex;
    int rightSplitOffsetPivot = NodeHelper::headerSize + 2 * sizeof(int) * rightSplitOffsetIndex;
    int leftSplitRecordBegin = 0;
    memcpy(&leftSplitRecordBegin, originalPage + leftSplitOffsetPivot, sizeof(int));
    //copy the middle key out
    int middleKeyLength = 0;
    memcpy(&middleKeyLength, originalPage + leftSplitOffsetPivot + sizeof(int), sizeof(int));
    memcpy(middleKey, originalPage + leftSplitRecordBegin, middleKeyLength);

    int rightSplitRecordBegin = leftSplitRecordBegin;
    memcpy(&rightSplitRecordBegin, originalPage + rightSplitOffsetPivot, sizeof(int));
    rightSplitRecordBegin -= sizeof(int);

    int compactSize = PAGE_SIZE - leftSplitRecordBegin;
    int rightTransferRecordSize = PAGE_SIZE - rightSplitRecordBegin;
    int transferOffsetCount = count - rightSplitOffsetIndex;

    //transfer record and offset
    memcpy(newPage + rightSplitRecordBegin, originalPage + rightSplitRecordBegin, rightTransferRecordSize);
    memcpy(newPage + NodeHelper::headerSize, originalPage + rightSplitOffsetPivot, transferOffsetCount * 2 * sizeof(int));

    //set Count and Memory
    NodeHelper::setCountAndSpace(originalPage, -transferOffsetCount - 1, -compactSize - (transferOffsetCount + 1) * 2 * sizeof(int));
    NodeHelper::setCountAndSpace(newPage, transferOffsetCount, rightTransferRecordSize + transferOffsetCount * 2 * sizeof(int));

    //compact the original data
    NodeHelper::compact(originalPage, NodeHelper::findLeftMostPoint(originalPage), leftSplitRecordBegin, compactSize);

    //update the offset position
    NodeHelper::changeOffsetValue(originalPage, leftSplitOffsetIndex, compactSize);
}

//get the next Entry
RC IX_ScanIterator::getNextEntry(RID &rid, void *key){
    //end of scan
    if (this->curPageNum == IX_EOF){
        return -1;
    }
    char* pair = (char*) calloc(PAGE_SIZE, sizeof(char));
    LeafHelper::getPair(this->currentPage, pair, this->curSlotNum);
    if (!isKeyValid(pair)){
        free(pair);
        return IX_EOF;
    }

    CommonHelper::getKeyAndRIDFromPair(pair, this->attr, rid, key);

    increaseRID();
    free(pair);
    return 0;
}

//check the right bound of the key, only the right bound
bool IX_ScanIterator::isKeyValid(char* key){
    if (this->highKey == nullptr) return true;

    float diff = CommonHelper::compare(this->highKey, key, this->attr);

    return this->highKeyInclusive ? diff >= 0 : diff > 0;
}

RC IX_ScanIterator::close(){
    free(this->currentPage);
    if (this->lowKey != nullptr) free(this->lowKey);
    if (this->highKey != nullptr) free(this->highKey);
    return 0;
}

//init the scan iterator
RC IX_ScanIterator::initScanIterator(IXFileHandle &ixFileHandle,
                                     const Attribute attribute,
                                     const void *lowKey,
                                     const void *highKey,
                                     bool lowKeyInclusive,
                                     bool highKeyInclusive){
    ixFileHandle.refreshHiddenPage();
    if(ixFileHandle.fp == NULL)
        return -1;

    this->attr = attribute;

    int strLength = 0;
    if (lowKey != nullptr){
        this->lowKey = (char*) calloc(PAGE_SIZE, sizeof(char));
        switch(attribute.type){
            case TypeVarChar:
                memcpy(&strLength, lowKey, sizeof(int));
                memcpy(this->lowKey, lowKey, sizeof(int) + strLength);
                break;
            case TypeInt:
            case TypeReal:
                memcpy(this->lowKey, lowKey, sizeof(int));
                break;
        }
    }
    if (highKey != nullptr){
        this->highKey = (char*) calloc(PAGE_SIZE, sizeof(char));
        switch(attribute.type){
            case TypeVarChar:
                memcpy(&strLength, highKey, sizeof(int));
                memcpy(this->highKey, highKey, sizeof(int) + strLength);
                break;
            case TypeInt:
            case TypeReal:
                memcpy(this->highKey, highKey, sizeof(int));
                break;
        }
    }

    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;
    this->curPageNum = ixFileHandle.getRoot();
    this->curSlotNum = 0;
    this->ixFileHandle = &ixFileHandle;
    this->currentPage = (char*) calloc(PAGE_SIZE, sizeof(char));

    findLeftKeyPosition();
    return 0;
}

//when the scan init, find the left most scan position
void IX_ScanIterator::findLeftKeyPosition(){
    if (this->curPageNum == -1) return;

    int rc = this->ixFileHandle->readPage(this->curPageNum, this->currentPage);
    if (rc < 0) return;
    if (LeafHelper::isLeaf(this->currentPage)){
        this->curCount = LeafHelper::getCount(currentPage);
        // when lowKey is nullptr, the slotnumber should be 0, which default value is 0 already
        if (this->lowKey == nullptr){
            //skip if the first page is empty
            while (this->curSlotNum >= this->curCount) {
                this->curPageNum = LeafHelper::getNext(this->currentPage);
                if (this->curPageNum == IX_EOF) return;
                ixFileHandle->readPage(this->curPageNum, this->currentPage);
                this->curSlotNum = 0;
                this->curCount = LeafHelper::getCount(this->currentPage);
            }
            return;
        }
        int recordBegin = 0, leftMostRecord = 0; // serve no function here
        float diff = LeafHelper::findKeyPosition(currentPage, this->attr, lowKey, this->curSlotNum, recordBegin, leftMostRecord, true);

        char* oldKey = (char*) calloc(PAGE_SIZE, sizeof(char));
        //when the lowkey is not inclusive, scan until the value satisfy
        while (!this->lowKeyInclusive && diff == 0){
            increaseRID();
            LeafHelper::getPair(this->currentPage, oldKey, this->curSlotNum);
            diff = LeafHelper::compare(this->lowKey, oldKey, this->attr);
        }
        //the key is not exist in this leaf page
        if (this->curSlotNum == this->curCount){
            increaseRID();
        }
        free(oldKey);
    } else {
        //when it is node, recursive to the leaf position
        if (this->lowKey == nullptr){
            this->curPageNum = NodeHelper::getFirstPage(this->currentPage);

        } else {
            this->curPageNum = NodeHelper::findPage(this->currentPage, this->attr, this->lowKey);
        }
        findLeftKeyPosition();
    }
}

//increase the slot count and go to next page if necessary
void IX_ScanIterator::increaseRID(){
    this->curSlotNum++;
    //needs to switch to next page
    while (this->curSlotNum >= this->curCount){
        this->curPageNum = LeafHelper::getNext(this->currentPage);
        if (this->curPageNum == IX_EOF) return;
        ixFileHandle->readPage(this->curPageNum, this->currentPage);
        this->curSlotNum = 0;
        this->curCount = LeafHelper::getCount(this->currentPage);
    }
}

//for test purpose, print current page
void NodeHelper::testPrint(char* nodeData){
    int count = NodeHelper::getCount(nodeData);
    std::cout<<count<<std::endl;
    int pivot = 0,dataBegin = 0,key = 0;
    for(int i = 0; i < count; i++){
        pivot = NodeHelper::headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, nodeData + pivot, sizeof(int));
        memcpy(&key, nodeData + dataBegin, sizeof(int));
        std::cout<<key<<" ";
        pivot = 0;
    }
    std::cout<<std::endl;
    for(int i = 0; i < count; i++){
        pivot = NodeHelper::headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, nodeData + pivot, sizeof(int));
        memcpy(&key, nodeData + dataBegin - sizeof(int), sizeof(int));
        std::cout<<key<<" ";
        pivot = 0;
    }
    std::cout<<std::endl;
}
//for test purpose, print current page
void LeafHelper::testPrint(char* leafData){
    int count = LeafHelper::getCount(leafData);
    std::cout<<count<<std::endl;
    int pivot = 0,dataBegin = 0,key = 0;
    for(int i = 0; i < count; i++){
        pivot = LeafHelper::headerSize + i * 2 * sizeof(int);
        memcpy(&dataBegin, leafData + pivot, sizeof(int));
        memcpy(&key, leafData + dataBegin, sizeof(int));
        std::cout<<key<<" ";
        pivot = 0;
    }
}

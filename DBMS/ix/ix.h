#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IXFileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter = 0;
    unsigned ixWritePageCounter = 0;
    unsigned ixAppendPageCounter = 0;
    unsigned pageNumCounter = 0;
    unsigned root = 0;
    FILE* fp = NULL;
    std::string fileName;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC refreshHiddenPage();
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();
    int getRoot();
    void setRoot(int newRoot);

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
private:
    RC insertWithPageNum(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int pageNum);
    RC deleteWithPageNum(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int pageNum);

    RC insertToNodeWithPage(IXFileHandle &ixFileHandle, int nodePageID,const Attribute& attr, const char* key, int newpageID);
    RC updateParentInChildren(IXFileHandle &ixFileHandle, char* data, int pageID);

};

class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator() = default;

    // Destructor
    ~IX_ScanIterator() = default;

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    RC initScanIterator(IXFileHandle &ixFileHandle,
                        const Attribute attribute,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive);

private:
    void increaseRID();
    bool isKeyValid(char* key);
    void findLeftKeyPosition();
    int curPageNum = 0;
    int curSlotNum = 0;
    int curCount = 0;
    char* currentPage;
    IXFileHandle* ixFileHandle = NULL;
    Attribute attr;
    char *lowKey = 0;
    char *highKey = 0;
    bool lowKeyInclusive;
    bool highKeyInclusive;
};


// All methods below should be static

class CommonHelper{
public:
    static bool isLeaf(char* data);
    static int getFreeSpace(char* data);
    static int getCount(char* data);
    static int getParent(char* data);
    static void setParent(char* data, int pageID);
    static void setCountAndSpace(char* data, int countDelta, int spaceDelta);
    static float findKeyPosition(char* leafData, const Attribute& attr, const char* key, int& offsetIndex, int& recordBegin, int& leftMostRecord, int headerSize, bool inclusive);
    static void compact(char* leafData, int leftMostRecord, int rightRecordPos, int delta);
    static void insertOffset(char* leafData, int index, int newDataBegin, int newDataLength, int headerSize);
    static void deleteOffset(char* leafData,int index, int delta, int headerSize);
    static void changeOffsetValue(char* leafData, int offsetRightEnd, int delta, int headerSize);
    static float compare(const char* newKey, const char* oldKey, const Attribute& attr);

    static void getFirstPair(char* data, char* key, int headerSize);
    static void getPair(char* data, char* key, int index, int headerSize);
    static void getKeyAndRIDFromPair(char* pair, const Attribute& attr, RID& rid, void* key);
    static int findLeftMostPoint(char* data,int headerSize);

};

class LeafHelper{
public:
    //size of the head is Layer - Free - Count - Parent - Previous - Next - 2*Hidden
    static const int headerSize = 8 * sizeof(int);

    static bool isLeaf(char* data){return CommonHelper::isLeaf(data);}
    static int getFreeSpace(char* data){return CommonHelper::getFreeSpace(data);}
    static int getCount(char* data){return CommonHelper::getCount(data);}
    static int getParent(char* data){return CommonHelper::getParent(data);}
    static void setParent(char* data, int pageID){CommonHelper::setParent(data, pageID);}
    static int getPrevious(char* data);
    static int getNext(char* data);
    static void setNext(char* data, int pageID);

    static bool haveSpace(char* data, const Attribute& attr, const char* key);
    static bool needMerge(char* data, const Attribute& attr);

    static float compare(const char* newKey, const char* oldKey, const Attribute& attr){
        return CommonHelper::compare(newKey, oldKey, attr);
    }
    static float findKeyPosition(char* leafData, const Attribute& attr, const char* key, int& offsetIndex, int& recordBegin, int& leftMostRecord, bool inclusive){
        return CommonHelper::findKeyPosition(leafData, attr,key,offsetIndex, recordBegin, leftMostRecord, LeafHelper::headerSize, inclusive);
    }
    static void compact(char* leafData, int leftMostRecord, int rightRecordPos, int delta){
        CommonHelper::compact(leafData, leftMostRecord, rightRecordPos, delta);
    }
    static void insertOffset(char* leafData, int index, int newDataBegin, int newDataLength){
        CommonHelper::insertOffset(leafData, index, newDataBegin, newDataLength, LeafHelper::headerSize);
    }
    static void deleteOffset(char* leafData,int index, int delta){
        CommonHelper::deleteOffset(leafData, index, delta, LeafHelper::headerSize);
    }
    static void changeOffsetValue(char* leafData, int offsetRightEnd, int delta){
        CommonHelper::changeOffsetValue(leafData, offsetRightEnd, delta, LeafHelper::headerSize);
    }

    static void print(char* data, const Attribute &attribute);
    static void testPrint(char* leafData);

    static void setCountAndSpace(char* data, int countDelta, int spaceDelta){
        CommonHelper::setCountAndSpace(data, countDelta, spaceDelta);
    }
    static int findLeftMostPoint(char* data){ return CommonHelper::findLeftMostPoint(data, LeafHelper::headerSize);}

    //create a empty leaf page
    static void createLeaf(char* data, int parent, int previous, int next);

    //insert the key in to the right position of this leaf
    static void insertToLeaf(char* leafData, const Attribute& attr, const char* key, const RID& rid);

    //delete the key inside the leaf, return -1 if not found
    static RC deleteInLeaf(char* leafData, const Attribute& attr, const char* key, const RID& rid);

    //when the page is full, call this function to split the leaf into two
    static void splitLeaf(char* originalPage, char* newPage);

    //get the first key in this page, serve for indexing
    static void getFirstPair(char* data, char* key){CommonHelper::getFirstPair(data, key, LeafHelper::headerSize);}
    static void getPair(char* data, char* key, int index){CommonHelper::getPair(data, key, index, LeafHelper::headerSize);}
};

class NodeHelper{
public:
    //size of the head is Layer - Free - Count - Parent - 2*Hidden
    static const int headerSize = 6 * sizeof(int);

    static bool isNode(char* data) {return !CommonHelper::isLeaf(data);}
    static int getFreeSpace(char* data) {return CommonHelper::getFreeSpace(data);}
    static int getCount(char* data) {return CommonHelper::getCount(data);}
    static int getParent(char* data) {return CommonHelper::getParent(data);}
    static void setParent(char* data, int pageID){return CommonHelper::setParent(data, pageID);}
    static void setCountAndSpace(char* data, int countDelta, int spaceDelta){CommonHelper::setCountAndSpace(data, countDelta, spaceDelta);}
    static void compact(char* data, int leftMostRecord, int rightRecordPos, int delta) {
        CommonHelper::compact(data, leftMostRecord, rightRecordPos, delta);
    }
    static float findKeyPosition(char* data, const Attribute& attr, const char* key, int& offsetIndex, int& recordBegin, int& leftMostRecord, bool inclusive){
        return CommonHelper::findKeyPosition(data, attr,key,offsetIndex, recordBegin, leftMostRecord, NodeHelper::headerSize,inclusive);
    }
    static void insertOffset(char* data, int index, int newDataBegin, int newDataLength){
        CommonHelper::insertOffset(data, index, newDataBegin, newDataLength, NodeHelper::headerSize);
    }
    static int findLeftMostPoint(char* data){ return CommonHelper::findLeftMostPoint(data, NodeHelper::headerSize) - sizeof(int);} // there is one PageID at the left of first record
    static void changeOffsetValue(char* data, int offsetRightEnd, int delta){
        CommonHelper::changeOffsetValue(data, offsetRightEnd, delta, NodeHelper::headerSize);
    }

    static void print(IXFileHandle &ixFileHandle, const Attribute &attribute, int position, int depth);
    static void testPrint(char* nodeData);

    static int getLayer(char* data);
    static void createNodeWithKey(char* data, int layer, int parent, int leftPageID, char* firstKey, int rightPageID, Attribute attr);
    static void createEmptyNode(char* data, int layer, int parent);
    static int findPage(char* data, const Attribute& attr, const char* key);
    static int getFirstPage(char* data);
    static void insertKey(char* data, const Attribute& attr, const char* key, int pageID);
    static bool haveSpace(char* data, const Attribute& attr, const char* key);
    static void splitNode(char* originalPage, char* newPage, char* middleKey);
};

#endif
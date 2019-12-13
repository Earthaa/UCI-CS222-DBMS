#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include "../ix/ix.h"
#include "../rbf/rbfm.h"

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() = default;

    ~RM_ScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC initScanIterator(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
        const std::string& conditionAttribute, const CompOp& compOp, const void* value,
        const std::vector<std::string>& attributeNames);

    RC close();

private:
    RBFM_ScanIterator rbfm_iter;    
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator() {};    // Constructor
    ~RM_IndexScanIterator() {};    // Destructor

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) ;    // Get next matching entry
    RC initScanIterator(const std::string &tableName,
                        const std::string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive
                        );
    RC close() { return ix_ScanIterator.close();


    };                        // Terminate index scan

private:
    IX_ScanIterator ix_ScanIterator;
    IXFileHandle ixFileHandle;
};

// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    // Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    static const std::string tableName;
    static const std::string columnName;
    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static int tableID;
    static const std::vector<Attribute> tableAttr;
    static const std::vector<Attribute> columnAttr;
    static const std::vector<Attribute> tableIndexMetaAttribute;

    //store only the last table name with its attributes to prevent repeated read and speed up getAttribute()
    //Memory usage is still O(1) since only the last one is stored
    std::string cacheTableName = "";
    std::vector<Attribute> cacheAttr;
    std::string cacheIndexTableName = "";
    std::vector<Attribute> cacheIndexAttr;

    FileHandle tableFileHandler;
    FileHandle columnFileHandler;
    RC insertTableMetaData(const std::string &tableName, const std::vector<Attribute> &attrs, const int mark = 0);
    RC openCatalog();
    RC closeCatalog();
    static RelationManager *_relation_manager;

    RC findTable(const std::string &tableName, char* data);
    RC findColumn(std::vector<Attribute>& attrs, const int ID);

    RC insertTableIndex(const std::string &tableName, const void *data, RID &rid);
    RC deleteTableIndex(const std::string &tableName, const RID &rid);

    RC getIndexAttributes(const std::string& tableName, std::vector<Attribute>& indexAttributes);

    RC deleteInTableFile(const std::string &tableName, int& ID);
    RC deleteInColumnFile(const int ID);
    bool isAttributeExist(const std::string& tableName, const std::string& attributeName);

};
class RMDataHelper{
public:
    static std::string getFileName(const std::string &tableName);
    static void createTableRaw(char* data, const std::string &tableName, const int ID, const int Mark = 0, const int version = 0);
    static void createColumnRaw(char* data, const int ID, const Attribute& attr, const int position, const int version = 0, const int firstCreatedVersion = 0);
    static int getTableID(char* data);

    static std::string getIndexFileName(const std::string &tableName, const std::string& attributeName);
    static std::string getIndexMetaTableName(const std::string &tableName);
    static void createAttributeRaw(char* data, const Attribute& attribute);
};

#endif

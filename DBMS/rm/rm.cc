#include "rm.h"
#include <functional>
#include <unordered_map>
using namespace std;
RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() = default;

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

int RelationManager::tableID = 0;
const std::string RelationManager::tableName = "Tables";
const std::string RelationManager::columnName = "Columns";

const std::vector<Attribute> RelationManager::tableAttr = std::vector<Attribute>{
        Attribute{"table-id", TypeInt, 4},
        Attribute{"table-name", TypeVarChar, 50},
        Attribute{"file-name", TypeVarChar, 50},
        Attribute{"mark", TypeInt, 4},
        Attribute{"current-version", TypeInt, 4},

};
const std::vector<Attribute> RelationManager::columnAttr = std::vector<Attribute>{
                                                                                Attribute{"table-id", TypeInt, 4},
                                                                                Attribute{"column-name", TypeVarChar, 50},
                                                                                Attribute{"column-type", TypeInt, 4},
                                                                                Attribute{"column-length", TypeInt, 4},
                                                                                Attribute{"column-position", TypeInt, 4},
                                                                                Attribute{"version", TypeInt, 4},
                                                                                Attribute{"first-created-version", TypeInt, 4}
                                                                                };

const std::vector<Attribute> RelationManager::tableIndexMetaAttribute = std::vector<Attribute>{
        Attribute{"attribute-name", TypeVarChar, 50},
        Attribute{"attribute-type", TypeInt, 4},
        Attribute{"attribute-length", TypeInt, 4}
};

RC RelationManager::createCatalog() {
    RC rc;
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

    rc = rbfm.createFile(RelationManager::tableName);
    if (rc < 0 ) {
        cerr<<"Table can not be created!"<<endl;
        return rc;

    }
    rc = rbfm.createFile(RelationManager::columnName);
    if (rc < 0 ) {
        cerr<<"Column can not be created!"<<endl;
        return rc;
    }


    insertTableMetaData(RelationManager::tableName, RelationManager::tableAttr, 1);
    insertTableMetaData(RelationManager::columnName, RelationManager::columnAttr,1);
    return rc;
}

RC RelationManager::deleteCatalog() {
    RC rc;
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

    this->cacheTableName = "";
    this->cacheAttr.clear();

    rc = rbfm.destroyFile(RelationManager::tableName);
    if (rc < 0 ) return rc;
    rc = rbfm.destroyFile(RelationManager::columnName);
    if (rc < 0 ) return rc;



    return rc;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {

    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc;
    //create the table raw data and insert into the table file
    //create the real file for the record

    std::string fileNameStr = RMDataHelper::getFileName(tableName);
    rc = rbfm.createFile(fileNameStr);
    if (rc != 0 ) return rc;

    std::string indexMetaTableName = RMDataHelper::getIndexMetaTableName(tableName);
    std::string indexMetaTableFileName = RMDataHelper::getFileName(indexMetaTableName);
    rc = rbfm.createFile(indexMetaTableFileName);
    if(rc != 0) return rc;

    //Insert table meta data to table and column
    rc = insertTableMetaData(tableName, attrs);
    if(rc != 0) return rc;

    //Insert table index meta data to table and column
    rc = insertTableMetaData(indexMetaTableName, RelationManager::tableIndexMetaAttribute);

    return rc;
}

// This method is used to insert table meta data to table and column
RC RelationManager::insertTableMetaData(const std::string &tableName, const std::vector<Attribute> &attrs, const int mark) {

    RC rc;
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    rc = openCatalog();
    if (rc < 0 ) return rc;
    char* tableData = (char*)calloc(PAGE_SIZE, sizeof(char));
    RID rid; //this rid serve no function
    
    std::hash<std::string> hasher;//hash the name to make it unique //test
    int tableID = (RelationManager::tableID++) + hasher(tableName);
    
    RMDataHelper::createTableRaw(tableData, tableName, tableID);
    rc = rbfm.insertRecord(this->tableFileHandler, RelationManager::tableAttr, tableData, rid);
    if (rc < 0) {
        free(tableData);
        return rc;
    }
    //cout<<attrs.size()<<endl;
    //create each of the column raw data and insert into the column file
    char* columnData = (char*)calloc(PAGE_SIZE, sizeof(char));
    for (int i = 0; i < attrs.size(); i++){
        RMDataHelper::createColumnRaw(columnData, tableID, attrs.at(i), i);
        rc = rbfm.insertRecord(this->columnFileHandler, RelationManager::columnAttr, columnData, rid);
        if (rc < 0) {
            free(tableData);
            free(columnData);
            return rc;
        }
    }
    free(tableData);
    free(columnData);
    rc = closeCatalog();
    return rc;
}
// You can't delete table or column or index by using this function !!

RC RelationManager::deleteTable(const std::string &tableName) {
    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    //delete the cache table name if matched
    if (tableName == this->cacheTableName){
        this->cacheTableName = "";
        this->cacheAttr.clear();
    }

    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    RC rc = openCatalog();
    if(rc != 0){
        return -1;
    }
    
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

    std::string fileNameStr = RMDataHelper::getFileName(tableName);
    rc = rbfm.destroyFile(fileNameStr);
    if (rc < 0){
        closeCatalog();
        return rc;
    }
    std::string indexMetaTableName = RMDataHelper::getIndexMetaTableName(tableName);
    rc = rbfm.destroyFile(RMDataHelper::getFileName(indexMetaTableName));
    if (rc < 0){
        closeCatalog();
        return rc;
    }

    int tableID = 0;
    rc = deleteInTableFile(tableName, tableID);


    //when table is not found in table file
    if (rc < 0){
        closeCatalog();
        return -1;
    }

    int indexTableID = 0;
    rc = deleteInTableFile(RMDataHelper::getIndexMetaTableName(tableName), indexTableID);
    if (rc < 0) return rc;
    rc = deleteInColumnFile(tableID);
    if (rc < 0) return rc;
    rc = deleteInColumnFile(indexTableID);
    if (rc < 0) return rc;

    rc = closeCatalog();
    return rc;

}


RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    //read last table name with its attributes to prevent repeated read and speed up getAttribute()
    //Memory usage is still O(1)
    if (tableName == this->cacheTableName){
        copy(this->cacheAttr.begin(), this->cacheAttr.end(), back_inserter(attrs)); 
        return 0;
    }

    RC rc;
    rc = openCatalog();
    if (rc < 0 ) return rc;

    //get the table data
    char* tableData = (char*)calloc(PAGE_SIZE, sizeof(char));
    rc = findTable(tableName, tableData);
    if (rc < 0 ) {
        free(tableData);
        return rc;
    }

    int ID = RMDataHelper::getTableID(tableData);
    // int currentVersion = getCurrentVersion(tableData);
    //get all the records for table-id, regardless the version
    rc = findColumn(attrs, ID);
    if (rc < 0 ){ free(tableData); return rc;}

    rc = closeCatalog();
    if (rc < 0 ) { free(tableData); return rc;}
    free(tableData);

    //store last table name with its attributes to prevent repeated read and speed up getAttribute()
    //only one is stored at the same time so Memory usage is still O(1)
    this->cacheTableName = tableName;
    this->cacheAttr.clear();
    copy(attrs.begin(), attrs.end(), back_inserter(this->cacheAttr)); 

    return rc;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    //IXFileHandle ixFileHandle;
    FileHandle fileHandle;

    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);

    if(rc != 0) return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0) return -1;
    rc = rbfm.insertRecord(fileHandle, attr, data, rid);
    rbfm.closeFile(fileHandle);

    std::vector<Attribute> indexAttrs;
    // Get the attributes have indexes on them
    rc = getIndexAttributes(tableName, indexAttrs);
    IndexManager& ixm = IndexManager::instance();
    if(rc != 0) return -1;

    // Insert to indexFile;
    for(Attribute attribute: indexAttrs){
        // With null byte at the beginning
        char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
        readAttribute(tableName, rid, attribute.name, rawAttribute);
        char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
        //skip null bit
        memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));

        string indexFileName = RMDataHelper::getIndexFileName(tableName, attribute.name);
        IXFileHandle ixFileHandle;
        rc = ixm.openFile(indexFileName, ixFileHandle);
        if(rc != 0){
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
            std::cerr<<"Can't open index file"<<endl;
            return -1;
        }
        rc = ixm.insertEntry(ixFileHandle, attribute, key, rid);
        if(rc != 0){
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
            std::cerr<<"Can't insert index"<<endl;
            return -1;
        }
        ixm.closeFile(ixFileHandle);
        free(key);
        free(rawAttribute);
    }


    return rc;
}

// It just insert to table without inserting to index
RC RelationManager::insertTableIndex(const std::string &tableName, const void *data, RID &rid) {

    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    //IXFileHandle ixFileHandle;
    FileHandle fileHandle;

    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);

    if(rc != 0) return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0) return -1;
    rbfm.insertRecord(fileHandle, attr, data, rid);

    rc = rbfm.closeFile(fileHandle);
    return rc;

}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = 0;
    std::vector<Attribute> attr;

    rc = getAttributes(tableName, attr);
    if(rc != 0)  return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0)  return -1;
    IndexManager& ixm = IndexManager::instance();
    std::vector<Attribute> indexAttrs;
    // Get the attributes have indexes on them
    rc = getIndexAttributes(tableName, indexAttrs);

    if(rc != 0) return -1;

    // Delete index
    for(Attribute attribute: indexAttrs){
        // With null byte at the beginning
        char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
        readAttribute(tableName, rid, attribute.name, rawAttribute);
        char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
        memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));
        string indexFileName = RMDataHelper::getIndexFileName(tableName, attribute.name);
        IXFileHandle ixFileHandle;
        rc = ixm.openFile(indexFileName, ixFileHandle);
        if(rc != 0){
            ixm.closeFile(ixFileHandle);
            std::cerr<<"Can't open index file"<<endl;
            return -1;
        }
        rc = ixm.deleteEntry(ixFileHandle, attribute, key, rid);
        if(rc != 0){
            ixm.closeFile(ixFileHandle);
            std::cerr<<"Can't delete index"<<endl;
            return -1;
        }
        ixm.closeFile(ixFileHandle);
        free(key);
        free(rawAttribute);
    }

    rc = rbfm.deleteRecord(fileHandle, attr, rid);
    if(rc != 0) return -1;

    rc = rbfm.closeFile(fileHandle);
    return rc;
}
// Normal delete
RC RelationManager::deleteTableIndex(const std::string &tableName, const RID &rid) {
    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = 0;
    std::vector<Attribute> attr;

    rc = getAttributes(tableName, attr);
    if(rc != 0)  return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0)  return -1;


    rc = rbfm.deleteRecord(fileHandle, attr, rid);
    if(rc != 0) return -1;

    rc = rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
        return -1;
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);
    if(rc != 0)
        return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0)
        return -1;
    rc = rbfm.updateRecord(fileHandle, attr, data, rid);
    if(rc != 0)
        return -1;
    rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {

    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);
    if(rc != 0)
        return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0)
        return -1;
    rc = rbfm.readRecord(fileHandle, attr, rid, data);
    if(rc != 0)
        return -1;
    rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    return rbfm.printRecord(attrs,data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);
    if(rc != 0)
        return -1;
    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0)
        return -1;
    rc = rbfm.readAttribute(fileHandle, attr, rid, attributeName, data);
    if(rc != 0)
        return -1;
    rbfm.closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    string fileName = RMDataHelper::getFileName(tableName);
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    fileHandle.fileName = fileName;
    RC rc = 0;
    std::vector<Attribute> attr;
    rc = getAttributes(tableName, attr);
    if(rc < 0) return -1;
    return rm_ScanIterator.initScanIterator(fileHandle, attr, conditionAttribute, compOp, value, attributeNames);
}

RC RM_ScanIterator::initScanIterator(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
    const std::string& conditionAttribute, const CompOp& compOp, const void* value,
    const std::vector<std::string>& attributeNames){
    return this->rbfm_iter.initScanIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    return this->rbfm_iter.getNextRecord(rid,data);
}

RC RM_ScanIterator::close(){
    return this->rbfm_iter.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

//open the table and column file, assign the this->tableFileHandler and this->columnFileHandler
RC RelationManager::openCatalog(){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc;
    rc = rbfm.openFile(RelationManager::tableName, this->tableFileHandler);
    if (rc < 0 ) return rc;
    rc = rbfm.openFile(RelationManager::columnName, this->columnFileHandler);
    if (rc < 0 ) return rc;
    return 0;
}

//close the table and column file
RC RelationManager::closeCatalog(){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc;
    rc = rbfm.closeFile(this->tableFileHandler);
    if (rc < 0 ) return rc;
    rc = rbfm.closeFile(this->columnFileHandler);
    if (rc < 0 ) return rc;
    return 0;
}

//delete the table record in the table file
RC RelationManager::deleteInTableFile(const std::string &tableName, int& ID){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc = 0;
    RBFM_ScanIterator iter;
    std::string conditionAttribute = "table-name";
    //create the match value, which is the tableName here
    int nameLength = tableName.size();
    char* value = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(value, &nameLength, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), nameLength);
    //create the attribute name
    const std::vector<std::string> tableBasicInfo{"table-id"};
    rc = rbfm.scan(this->tableFileHandler, tableAttr, conditionAttribute, EQ_OP, value, tableBasicInfo, iter);
    if (rc < 0) return rc;

    free(value);
    RID rid;
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));
    rc = iter.getNextRecord(rid, data);
    iter.close();

    //when there is no record match
    if (rc < 0) {
        free(data);
        int error = -1;
        memcpy(&ID, &error, sizeof(int));
        return 0;
    }

    //delete the table in the table file
    rc = rbfm.deleteRecord(this->tableFileHandler, RelationManager::tableAttr, rid);
    if (rc < 0){
        free(data);
        return rc;
        }
    const int tableID = RMDataHelper::getTableID(data);
    memcpy(&ID, &tableID, sizeof(int));

    free(data);
    return rc;
}

//delete the column record in the column file
RC RelationManager::deleteInColumnFile(const int ID){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc = 0;
    RBFM_ScanIterator iter;
    std::string table_id = "table-id";
    std::vector<RID> rids;

    //create the match value, which is the table-id here
    char* value = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(value, &ID, sizeof(int));

    //we need nothing here, just put someting to prevent error
    std::vector<std::string> columnBasicInfo;
    columnBasicInfo.push_back("table-id");

    rc = rbfm.scan(this->columnFileHandler, columnAttr, table_id, EQ_OP, value, columnBasicInfo, iter);
    if (rc < 0 ) return rc;

    RID rid;
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));
    //save all the column record found in the column file
    while(iter.getNextRecord(rid, data) != RBFM_EOF){
        rids.push_back(rid);
    }
    free(data);
    free(value);
    iter.close();
    //delete all the records found in column file
    for(const RID r : rids){
        rc = rbfm.deleteRecord(this->columnFileHandler, this->columnAttr, r);
        if (rc < 0) return rc;
    }
    return 0;
}

//create the filename from the tableName, currently we use the same with table-name.
//return the fileName length
std::string RMDataHelper::getFileName(const std::string &tableName){
    return ((tableName == RelationManager::tableName) || (tableName == RelationManager::columnName) )? tableName :tableName + ".bin";
}

std::string RMDataHelper::getIndexMetaTableName(const std::string &tableName) {
    return tableName + "$" + "IndexMeta";
}

//create the raw data for the table, return the table-id
void RMDataHelper::createTableRaw(char* data, const std::string &tableName, const int ID ,const int mark, const int version){
    int pivot = 0;
    //nullByte
    char nullByte = 0x00;
    memcpy(data, &nullByte, sizeof(char));
    pivot += sizeof(char);
    //table-id
    memcpy(data + pivot, &ID, sizeof(int));
    pivot += sizeof(int);

    //table-name
    int nameLength = tableName.length();
    const char* name = tableName.c_str();
    memcpy(data + pivot, &nameLength, sizeof(int));
    pivot += sizeof(int);
    memcpy(data + pivot, name, nameLength);
    pivot += nameLength;

    //file-name
    // char fileName[PAGE_SIZE];
    std::string fileName = RMDataHelper::getFileName(tableName);
    int fileNameLength = fileName.size();
    const char* fileNameChar = fileName.c_str();
    memcpy(data + pivot, &fileNameLength, sizeof(int));
    pivot += sizeof(int);
    memcpy(data + pivot, fileNameChar, fileNameLength);
    pivot += fileNameLength;

    //mark
    memcpy(data + pivot, &mark, sizeof(int));
    pivot += sizeof(int);

    //currentVersion
    memcpy(data + pivot, &version, sizeof(int));

}

//create the raw data for the column
void RMDataHelper::createColumnRaw(char* data,const int ID, const Attribute& attr, const int position, int version, int firstCreatedVersion){
    int pivot = 0;
    //nullByte
    char nullByte = 0x0;
    memcpy(data, &nullByte, sizeof(char));
    pivot += sizeof(char);

    //table-id
    memcpy(data + pivot, &ID, sizeof(int));
    pivot += sizeof(int);

    //column-name
    int nameLength = attr.name.size();
    const char* name = attr.name.c_str();
    memcpy(data + pivot, &nameLength, sizeof(int));
    pivot += sizeof(int);
    memcpy(data + pivot, name, nameLength);
    pivot += nameLength;

    //column-type
    int type = attr.type;
    memcpy(data + pivot, &type, sizeof(int));
    pivot += sizeof(int);

    //column-length
    int columnLength = attr.length;
    memcpy(data + pivot, &columnLength, sizeof(int));
    pivot += sizeof(int);

    //column-position
    memcpy(data + pivot, &position, sizeof(int));
    pivot += sizeof(int);

    //version
    memcpy(data + pivot, &version, sizeof(int));
    pivot += sizeof(int);

    //FirstCreatedVersion
    memcpy(data + pivot, &firstCreatedVersion, sizeof(int));
    pivot += sizeof(int);

}

//scan the table to find the record matches the tableName, assuming only one exists
//only return ["table-id", "currentVersion", "file-name"]
RC RelationManager::findTable(const std::string &tableName, char* data){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc = 0;
    RBFM_ScanIterator iter;
    std::string conditionAttribute = "table-name";
    //create the match value, which is the tableName here
    int nameLength = tableName.size();
    char* value = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(value, &nameLength, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), nameLength);
    //create the attribute name
    const std::vector<std::string> tableBasicInfo{"table-id", "file-name", "current-version"};
    rc = rbfm.scan(this->tableFileHandler, tableAttr, conditionAttribute, EQ_OP, value, tableBasicInfo, iter);

    free(value);
    RID rid;// this rid serve no function;
    rc = iter.getNextRecord(rid, data);
    iter.close();

    return rc;
}

//scan the column to find the record matches the ID, assuming multiple exists
//only return ["version", "firstCreatedVersion", "column-position", "column-name", "column-type", "column-length"]
RC RelationManager::findColumn(std::vector<Attribute>& attrs, const int ID){
    RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
    RC rc;
    RBFM_ScanIterator iter;
    std::string table_id = "table-id";
    //create the match value, which is the table-id here
    char* value = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(value, &ID, sizeof(int));

    //create the attribute name
    std::vector<std::string> columnBasicInfo;
    for(int i = 0; i < RelationManager::columnAttr.size(); i++)
        columnBasicInfo.push_back(RelationManager::columnAttr[i].name);

    rc = rbfm.scan(this->columnFileHandler, columnAttr, table_id, EQ_OP, value, columnBasicInfo, iter);
    if (rc < 0 ) return rc;

    RID rid;
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));
    while(iter.getNextRecord(rid, data) != RBFM_EOF){
        unsigned length;
        char* recordData = (char*)calloc(PAGE_SIZE, sizeof(char));
        DataHelper::rawToRecord(length, recordData, data, RelationManager::columnAttr);
        //We need column-name, column-type and column-length for each attribute
        //column-name
        int indexOffset = 2 * sizeof(char) + sizeof(int);
        int start = indexOffset + sizeof(int) * RelationManager::columnAttr.size();
        int end;

        memcpy(&start, recordData + indexOffset, sizeof(int));
        indexOffset += sizeof(int);
        memcpy(&end, recordData + indexOffset, sizeof(int));
        Attribute attr;
        // minus 1 for we have an end byte
        attr.name = string(recordData + start, recordData + end - 1);
        //column type
        start = end;
        indexOffset += sizeof(int);
        memcpy(&end, recordData + indexOffset, sizeof(int));
        AttrType type;
        memcpy(&type, recordData + start, sizeof(int));
        attr.type = type;

        //column length
        start = end;
        indexOffset += sizeof(int);
        memcpy(&end, recordData + indexOffset, sizeof(int));
        int attrLength;
        memcpy(&attrLength, recordData + start, sizeof(int));
        attr.length = attrLength;

        attrs.push_back(attr);
        free(recordData);
    }
    free(data);
    free(value);
    iter.close();
    return 0;
}


int RMDataHelper::getTableID(char* data){
    int pivot = 1; //skip nullByte
    int res = 0;
    memcpy(&res, data + pivot, sizeof(int));
    return res;
}


std::string RMDataHelper::getIndexFileName(const std::string &tableName, const std::string &attributeName) {
    return tableName + "_" + attributeName + ".idx";
}


// Make attribute to be the tuple type data so that it can be inserted
void RMDataHelper::createAttributeRaw(char *data, const Attribute &attribute) {
    char nullByte = 0x00;

    memcpy(data, &nullByte, sizeof(char));
    int pivot = sizeof(char);

    int nameLength = attribute.name.size();
    memcpy(data + pivot, & nameLength, sizeof(int));
    pivot += sizeof(int);

    memcpy(data + pivot, attribute.name.c_str(),nameLength);
    pivot += nameLength;

    memcpy(data + pivot, &attribute.type, sizeof(int));
    pivot += sizeof(int);

    memcpy(data + pivot, &attribute.length, sizeof(int));

}

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {

    if (tableName == this->cacheIndexTableName){
        this->cacheIndexTableName = "";
        this->cacheIndexAttr.clear();
    }

    if(!isAttributeExist(tableName, attributeName)) {
        cerr<<"Table or attribute Not Found!"<<endl;
        return -1;
    }
    // No matter it exists or not, just delete

    // Create indexFile
    std::string indexFileName = RMDataHelper::getIndexFileName(tableName, attributeName);
    IndexManager& indexManager = IndexManager::instance();
    RC rc = indexManager.createFile(indexFileName);
    if(rc != 0)
        return -1;
    IXFileHandle ixFileHandle;
    rc = indexManager.openFile(indexFileName, ixFileHandle);
    if(rc != 0) return -1;
    RM_ScanIterator iter;
    vector<std::string> attributeNames;

    //AttributeNames only contains the attribute name of which we wants to create an index on
    attributeNames.push_back(attributeName);
    // just for the API, it's useless
    // We should figure out the specific attribute
    vector<Attribute> attributes;
    getAttributes(tableName,attributes);
    Attribute attribute;
    for(int i = 0; i < attributes.size(); i++){
        if(attributes[i].name == attributeName)
            attribute = attributes[i];
    }

    // Insert this attribute to indexMetaFile
    char* attributeMetaData = (char*)calloc(PAGE_SIZE, sizeof(char));
    RMDataHelper::createAttributeRaw(attributeMetaData, attribute);

    RID arid;
    rc = insertTableIndex(RMDataHelper::getIndexMetaTableName(tableName),attributeMetaData, arid);
    if(rc != 0) return -1;

    // Create index on each record
    rc = scan(tableName, "", NO_OP, NULL, attributeNames, iter);
    if(rc != 0) return -1;
    RID rid;
    char* data = (char*)calloc(PAGE_SIZE, sizeof(char));

    while(iter.getNextTuple(rid, data) != RM_EOF){
        // For there is null byte in the front of data
        char*key = (char*)calloc(PAGE_SIZE, sizeof(char));
        // For keyLength > the true key Length and the remaining bytes will be ignored automatically
        int keyLength = attribute.length + sizeof(int);
        memcpy(key, data + sizeof(char), keyLength);
        indexManager.insertEntry(ixFileHandle, attribute, key, rid);
        free(key);
    }

    free(data);
    free(attributeMetaData);
    indexManager.closeFile(ixFileHandle);
    iter.close();

    return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {

    if (tableName == this->cacheIndexTableName){
        this->cacheIndexTableName = "";
        this->cacheIndexAttr.clear();
    }

    if(!isAttributeExist(tableName, attributeName))
        return -1;
    std::string indexFileName = RMDataHelper::getIndexFileName(tableName, attributeName);
    IndexManager& indexManager = IndexManager::instance();
    RC rc = indexManager.destroyFile(indexFileName);
    if(rc != 0) {
        return rc;
    }
    //Delete the corresponding record in meta file

    RelationManager& rm = RelationManager::instance();
    RM_ScanIterator iter;
    char* value = (char*)calloc(PAGE_SIZE, sizeof(int));
    int attributeNameLength = attributeName.size();
    memcpy(value, &attributeNameLength, sizeof(int));
    memcpy(value + sizeof(int), attributeName.c_str(), attributeNameLength);

    vector<std::string> attributeNames;
    for(Attribute attribute:RelationManager::tableIndexMetaAttribute){
        attributeNames.push_back(attribute.name);
    }

    rc = scan(RMDataHelper::getIndexMetaTableName(tableName), attributeName, EQ_OP, value, attributeNames, iter);
    if(rc != 0) return -1;

    RID rid;
    //Data is useless here
    char* data = (char*)calloc(PAGE_SIZE, sizeof(int));
    // There should be only one
    iter.getNextTuple(rid,data);
    rm.deleteTableIndex(RMDataHelper::getIndexMetaTableName(tableName), rid);

    iter.close();
    free(data);
    free(value);
    return rc;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {


    return rm_IndexScanIterator.initScanIterator(tableName, attributeName, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

bool RelationManager::isAttributeExist(const std::string &tableName, const std::string &attributeName) {
    // Whether tableName exists
    vector<Attribute> attributes;
    RC rc = getAttributes(tableName, attributes);
    if(rc != 0) return false;
    for(Attribute attribute:attributes){
        if(attribute.name == attributeName)
            return true;
    }
    return false;

}

RC RelationManager::getIndexAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    // if the indexTable is cached, return directly
    if (tableName == this->cacheIndexTableName){
        copy(this->cacheIndexAttr.begin(), this->cacheIndexAttr.end(), back_inserter(attrs)); 
        return 0;
    }

    RM_ScanIterator iter;
    vector<std::string> metaAttributeName;
    std::string indexMetaTableName = RMDataHelper::getIndexMetaTableName(tableName);
    for(int i = 0; i < RelationManager::tableIndexMetaAttribute.size(); i++){
        metaAttributeName.push_back(RelationManager::tableIndexMetaAttribute[i].name);
    }
    // value is useless just for api
    // just scan the table
    scan(indexMetaTableName,"",NO_OP, NULL, metaAttributeName,iter);
    RID rid;
    char* attributeData = (char*)calloc(PAGE_SIZE, sizeof(char));

    this->cacheIndexTableName = tableName;
    this->cacheIndexAttr.clear();

    while(iter.getNextTuple(rid, attributeData) != RM_EOF){
        // skip null bytes
        int pivot = 1;
        int nameLength;
        memcpy(&nameLength, attributeData + pivot, sizeof(int));
        pivot += sizeof(int);
        string attributeName = string(attributeData + pivot, attributeData + pivot + nameLength);
        pivot += nameLength;

        int type;
        memcpy(&type, attributeData + pivot, sizeof(int));
        pivot += sizeof(int);

        int length;
        memcpy(&length, attributeData + pivot, sizeof(int));

        Attribute attr;
        attr.name = attributeName;
        attr.type = (AttrType)type;
        attr.length = length;
        attrs.push_back(attr);
        this->cacheIndexAttr.push_back(attr);
    }
    free(attributeData);
    iter.close();
    return 0;

}

RC RM_IndexScanIterator::initScanIterator(const std::string &tableName,
                    const std::string &attributeName,
                    const void *lowKey,
                    const void *highKey,
                    bool lowKeyInclusive,
                    bool highKeyInclusive
){
      std::string indexFileName = RMDataHelper::getIndexFileName(tableName, attributeName);
      Attribute attribute;
      RelationManager& rm = RelationManager::instance();

      vector<Attribute> attributes;
      RC rc;
      rc = rm.getAttributes(tableName, attributes);
      if(rc != 0){
          std::cerr<<"No such table!"<<endl;
          return rc;
      }
      // find the attribute with the specific name
      for(Attribute attr:attributes){
          if(attr.name == attributeName) {
              attribute = attr;
              break;
          }
      }
      rc = IndexManager::instance().openFile(indexFileName, ixFileHandle);
      if(rc != 0){
          std::cerr<<"No such index table!"<<endl;
          return rc;
      }
      return IndexManager::instance().scan(ixFileHandle,attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, this->ix_ScanIterator);
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
    return this->ix_ScanIterator.getNextEntry(rid,key);
}
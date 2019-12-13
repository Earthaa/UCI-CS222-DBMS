#ifndef _qe_h_
#define _qe_h_

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#include <algorithm>
#include <limits>
#include <map>
#include <vector>

#define QE_EOF (-1)  // end of the index scan
using namespace std;
typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;
// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

class QEDataHelper{
public:
    static bool isSatisfy(char* data, const std::vector<Attribute>& attrs, const Condition& cond);
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;
};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName, rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;


        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
    };
};

class Filter : public Iterator {
    // Filter operator
public:
    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );

    ~Filter() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        input->getAttributes(attrs);
    };
private:
    Iterator* input;
    Condition condition;
    std::vector<Attribute> attributes;
};

class Project : public Iterator {
    // Projection operator
public:
    Project(Iterator *input,                    // Iterator of input R
            const std::vector<std::string> &attrNames) {
        this->attrNames = attrNames;
        this->input = input;
        input->getAttributes(this->attributes);
    };   // std::vector containing attribute names
    ~Project() override = default;

    RC getNextTuple(void *data) override ;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        std::vector<Attribute> temp;
        input->getAttributes(temp);
        for (std::string s : this->attrNames){
            for(Attribute attr : temp){
                if (attr.name == s){
                    attrs.push_back(attr);
                    break;
                }
            }
        }
    };
private:
    Iterator* input;
    vector<std::string> attrNames;
    std::vector<Attribute> attributes;
};

class JoinHelper{
public:
    static bool isSatisfy(char* leftData, char* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs, const Condition& cond);
    static AttrType findKey(char* key, char* data, const std::vector<Attribute>& attrs, const std::string name);
    static void join(char* result, char* leftData, char* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs);
    static AttrType findType(const std::vector<Attribute>& attrs, const std::string name);
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
public:
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    ) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);

        this->maxSize = numPages * PAGE_SIZE;
        this->remainSize = numPages * PAGE_SIZE;

        //find the max size single tuple could be
        this->singlePosibileSize = 0;
        for (int i = 0; i < this->leftAttrs.size(); i++){
            this->singlePosibileSize += this->leftAttrs.at(i).length;
        }

        this->attrType = JoinHelper::findType(this->leftAttrs, this->condition.lhsAttr);
        //init the value
        updateLeftMap();
        this->currentRightPivot = 0;
        this->rightIn->getNextTuple(this->currentRightData);
    };

    ~BNLJoin() override {free(this->currentRightData);};

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {
        attrs.insert(attrs.end(), this->leftAttrs.begin(), this->leftAttrs.end());
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
    };

    bool hasSpace(){
        //TODO need better algorithm
        return this->remainSize > this->singlePosibileSize;
    };
    bool updateLeftMap();
    int getSize(char* data, const std::vector<Attribute> &attrs);

private:
    Iterator* leftIn;
    TableScan* rightIn;
    Condition condition;
    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;
    int maxSize = 0, remainSize = 0;
    int singlePosibileSize = 0;
    std::map<int, std::vector<std::vector<char>>> intMap;
    std::map<float, std::vector<std::vector<char>>> floatMap;
    std::map<std::string, std::vector<std::vector<char>>> strMap;
    char* currentRightData = (char*) calloc(PAGE_SIZE, sizeof(char*));
    int currentRightPivot = 0;
    AttrType attrType;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
public:
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    ) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);

        this->currentLeftStatus = this->leftIn->getNextTuple(this->currentLeftData);
    };

    ~INLJoin() override {free(this->currentLeftData);};

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override{
        attrs.insert(attrs.end(), this->leftAttrs.begin(), this->leftAttrs.end());
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
    }
private:
    Iterator* leftIn;
    IndexScan* rightIn;
    Condition condition;
    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;
    char* currentLeftData = (char*) calloc(PAGE_SIZE, sizeof(char*));
    int currentLeftStatus = 0;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    ) {};

    ~GHJoin() override = default;

    RC getNextTuple(void *data) override { return QE_EOF; };

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override {};
};

class Aggregate : public Iterator {
    // Aggregation operator
public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    ) {
        this->single = true;
        this->finished = false;
        this->sum = (op == MIN ? std::numeric_limits<float>::max() : 0);
        this->count = 0;
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
    };

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    ){
        single = false;
    };

    ~Aggregate() override = default;

    RC getNextTuple(void *data) override;

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override;

private:
    Iterator* input;
    Attribute aggAttr;
    std::vector<Attribute> attributes;
    bool single = true;
    bool finished = false;
    int count = 0;
    float sum = 0;
    AggregateOp op;
};


#endif

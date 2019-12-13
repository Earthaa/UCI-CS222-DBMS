#include "rm_test_util.h"

RC TEST_RM_15(const std::string &tableName)
{
    // Functions Tested:
    // 1. System Catalog Implementation - Columns table
    std::cout <<std::endl << "***** In RM Test Case 15 *****" <<std::endl;

    // Get Catalog Attributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    // There should be at least five attributes: table-id, column-name, column-type, column-length, column-position
    if (attrs.size() < 5) {
        std::cout << "Your system catalog schema is not correct." <<std::endl;
       std::cout << "***** [FAIL] Test Case 15 failed *****" <<std::endl;
        return -1;
    } else if (attrs[0].name != "table-id" || attrs[1].name != "column-name" ||
    		   attrs[2].name != "column-type" || attrs[3].name != "column-length" || attrs[4].name != "column-position") {
        std::cout << "Your system catalog schema is not correct." <<std::endl;
       std::cout << "***** [FAIL] Test Case 15 failed *****" <<std::endl;
        return -1;
    }

    RID rid;
    void *returnedData = malloc(200);

    // Set up the iterator
    RM_ScanIterator rmsi;
    std::vector<std::string> projected_attrs;
    for (Attribute & attr : attrs){
      projected_attrs.push_back(attr.name);
    }

    rc = rm.scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    int count = 0;
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
    	// We will manually check the returned tuples to see whether your implementation is correct or not.
        rm.printTuple(attrs, returnedData);
        count++;
    }
    rmsi.close();

    // There should be at least eight rows - three for Tables and five for Columns
    if (count < 8) {
        std::cout << "Your system catalog schema is not correct." <<std::endl;
       std::cout << "***** [FAIL] Test Case 15 failed" <<std::endl;
        free(returnedData);
        return -1;
    }

    // Deleting the catalog should fail.
    rc = rm.deleteTable(tableName);
    assert(rc != success && "RelationManager::deleteTable() on the system catalog table should fail.");

    free(returnedData);
   std::cout << "***** Test Case 15 Finished. The result will be examined. *****" <<std::endl;
    return 0;
}

int main()
{
    // NOTE: your Columns table must be called "Columns"
    std::string catalog_table_name = "Columns";

    // Test Catalog Information
    return TEST_RM_15(catalog_table_name);
}

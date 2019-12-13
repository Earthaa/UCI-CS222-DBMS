#include "rm_test_util.h"

RC TEST_RM_14(const std::string &tableName) {
    // Functions Tested:
    // 1. System Catalog Implementation - Tables table
    std::cout << std::endl << "***** In RM Test Case 14 *****" << std::endl;

    // Get Catalog Attributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    // There should be at least three attributes: table-id, table-name, file-name
    if (attrs.size() < 3) {
        std::cout << "Your system catalog schema is not correct." << std::endl;
        std::cout << "***** [FAIL] Test Case 14 failed *****" << std::endl;
        return -1;
    } else if (attrs[0].name != "table-id" || attrs[1].name != "table-name" || attrs[2].name != "file-name") {
        std::cout << "Your system catalog schema is not correct." << std::endl;
        std::cout << "***** [FAIL] Test Case 14 failed *****" << std::endl;
        return -1;
    }

    RID rid;
    void *returnedData = malloc(1000);

    // Set up the iterator
    RM_ScanIterator rmsi;
    std::vector<std::string> projected_attrs;
    projected_attrs.reserve(attrs.size());
    for (Attribute &attr : attrs) {
        projected_attrs.push_back(attr.name);
    }

    rc = rm.scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    int count = 0;
    while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
        // We will manually check the returned tuples to see whether your implementation is correct or not.
        rm.printTuple(attrs, returnedData);
        count++;
    }
    rmsi.close();

    // There should be at least two rows - one for Tables and one for Columns
    if (count < 2) {
        std::cout << "Your system catalog schema is not correct." << std::endl;
        std::cout << "***** [FAIL] Test Case 14 failed" << std::endl;
        free(returnedData);
        return -1;
    }

    // Deleting the catalog should fail.
    rc = rm.deleteTable(tableName);
    assert(rc != success && "RelationManager::deleteTable() on the system catalog table should fail.");

    free(returnedData);
    std::cout << "***** Test Case 14 Finished. The result will be examined. *****" << endl;
    return 0;
}

int main() {
    // NOTE: your Tables table must be called "Tables"
    std::string catalog_table_name = "Tables";

    // Test Catalog Information
    return TEST_RM_14(catalog_table_name);
}

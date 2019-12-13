#include "rm_test_util.h"

RC TEST_RM_0(const std::string &tableName) {
    // Functions Tested
    // 1. getAttributes **
    std::cout << std::endl << "***** In RM Test Case 0 *****" << std::endl;

    // GetAttributes
    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    for (unsigned i = 0; i < (unsigned) attrs.size(); i++) {
        std::cout << (i + 1) << ". Attr Name: " << attrs[i].name << " Type: " << (AttrType) attrs[i].type << " Len: "
                  << attrs[i].length << std::endl;
    }

    std::cout << std::endl << "***** RM Test Case 0 finished. The result will be examined. *****" << std::endl;

    return success;
}

int main() {
    // Get Attributes
    return TEST_RM_0("tbl_employee");
}

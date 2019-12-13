#include "rm_test_util.h"

int main() {

    // By executing this script, the following tables including the system tables will be removed.
    std::cout << std::endl << "***** RM TEST - Deleting the Catalog and User tables *****" << std::endl;

    RC rc = rm.deleteTable("tbl_employee");
    if (rc != 0) {
        std::cout << "Deleting tbl_employee failed." << std::endl;
    }

    rc = rm.deleteTable("tbl_employee2");
    if (rc != 0) {
        std::cout << "Deleting tbl_employee2 failed." << std::endl;
    }

    rc = rm.deleteTable("tbl_employee3");
    if (rc != 0) {
        std::cout << "Deleting tbl_employee3 failed." << std::endl;
    }

    rc = rm.deleteTable("tbl_employee4");
    if (rc != 0) {
        std::cout << "Deleting tbl_employee4 failed." << std::endl;
    }

    rc = rm.deleteCatalog();
    if (rc != 0) {
        std::cout << "Deleting the catalog failed." << std::endl;
        return rc;
    }

    return success;
}

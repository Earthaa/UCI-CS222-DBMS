#include "rm_test_util.h"

RC TEST_RM_12(const std::string &tableName)
{
    // Functions Tested for large tables
    // 1. scan
   std::cout <<std::endl << "***** In RM Test case 12 *****" <<std::endl;

    RM_ScanIterator rmsi;
    std::vector<std::string> attrs;
    attrs.emplace_back("attr5");
    attrs.emplace_back("attr12");
    attrs.emplace_back("attr28");
   
    RC rc = rm.scan(tableName, "", NO_OP, NULL, attrs, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    RID rid;
    int j = 0;
    void *returnedData = malloc(4000);

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        if(j % 200 == 0)
        {
            int offset = 0;

           std::cout << "Real Value: " << *(float *)((char *)returnedData+nullAttributesIndicatorActualSize) <<std::endl;
            offset += 4;
        
            int size = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
           std::cout << "Varchar size: " << size <<std::endl;
            offset += 4;

            char *buffer = (char *)malloc(size + 1);
            memcpy(buffer, (char *)returnedData + offset + nullAttributesIndicatorActualSize, size);
            buffer[size] = 0;
            offset += size;
    
           std::cout << "VarChar Value: " << buffer <<std::endl;

           std::cout << "Integer Value: " << *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize) <<std::endl <<std::endl;
            offset += 4;

            free(buffer);
        }
        j++;
        memset(returnedData, 0, 4000);
    }
    rmsi.close();
   std::cout << "Total number of tuples: " << j <<std::endl <<std::endl;
    if (j > 1000) {
       std::cout << "***** [FAIL] Test Case 12 Failed *****" <<std::endl <<std::endl;
        free(returnedData);
        return -1;
    }

   std::cout << "***** Test Case 12 Finished. The result will be examined. *****" <<std::endl <<std::endl;
    free(returnedData);

    return success;
}

int main()
{
	// Scan
    return TEST_RM_12("tbl_employee4");
}

#include "cli.h"

#define SUCCESS 0
#define MODE 0  // 0 = TEST MODE
// 1 = INTERACTIVE MODE
// 3 = TEST + INTERACTIVE MODE

CLI *cli;

void exec(const std::string &command, bool equal = true) {
    std::cout << ">>> " << command << std::endl;

    if (equal)
        assert (cli->process(command) == SUCCESS);
    else
        assert (cli->process(command) != SUCCESS);
}

// IndexScan
void Test11() {

    std::cout << "*********** CLI Test11 begins ******************" << std::endl;

    std::string command;

    exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

    exec("load employee employee_5");

    exec("create index Height on employee");
    exec("SELECT PROJECT IDXSCAN employee (Height > 5.7) GET [ * ]");
    exec("SELECT PROJECT IDXSCAN employee (Height < 6.4) GET [ * ]");
    exec("SELECT PROJECT IDXSCAN employee (Height = 6.6) GET [ * ]");

    exec(("drop table employee"));
}

int main() {

    cli = CLI::Instance();

    if (MODE == 0 || MODE == 3) {
        Test11(); // IndexScan
    }
    if (MODE == 1 || MODE == 3) {
        cli->start();
    }

    return 0;
}

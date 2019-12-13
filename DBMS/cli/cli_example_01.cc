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

// test create table
// test drop table
void Test01() {

    std::cout << "*********** CLI Test01 begins ******************" << std::endl;

    std::string command;

    exec("create catalog");

    exec("create table ekin name = varchar(40), age = int");

    exec("print cli_columns");

    exec("print cli_tables");

    exec("drop table ekin");

    exec("print cli_tables");

    exec("print cli_columns");

    std::cout << "We should not see anything related to ekin table" << std::endl;
}

int main() {

    cli = CLI::Instance();

    if (MODE == 0 || MODE == 3) {
        Test01();
    }
    if (MODE == 1 || MODE == 3) {
        cli->start();
    }

    return 0;
}

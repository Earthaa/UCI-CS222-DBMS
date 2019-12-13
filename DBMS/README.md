By default you should not change those functions of pre-defined in the given .h files.
If you think some changes are really necessary, please contact us first.

If you are not using CLion and want to use command line make tool:

 - Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base if you can't compile the code.
 
- Implement the extension of Relation Manager (RM) to coordinate data files and the associated indices of the data files.

- Also, implement Query Engine (QE)

   Go to folder "qe" and type in:

    make clean
    make
    ./qetest_01

   The program should work. But it does nothing until you implement the extension of RM and QE.

- If you want to try CLI:

   Go to folder "cli" and type in:
   
   make clean
   make
   ./cli_example_01
   
   or
   
   ./start
   
   The program should work. But you need to implement the extension of RM and QE to run this program properly. Note that examples in the cli directory are provided for your convenience. These examples are not the public test cases.

- By default you should not change those classes defined in rm/rm.h and qe/qe.h. If you think some changes are really necessary, please contact us first.
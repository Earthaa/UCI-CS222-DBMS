## For students: change this path to the root of your code if this doesn't work
CODEROOT = ..

#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11  # with debugging info and the C++11 feature

# Comment the following line to disable command line interface (CLI).
#CPPFLAGS = -Wall -I$(CODEROOT) -std=c++11 -ledit -DDATABASE_FOLDER=\"$(CODEROOT)/cli/\" -g # with debugging info

# Uncomment the following line to compile the code without using CLI.
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++0x  # with debugging info and the C++11 feature
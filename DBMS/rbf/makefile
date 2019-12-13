include ../makefile.inc

all: librbf.a rbftest_01 rbftest_02 rbftest_03 rbftest_04 rbftest_05 rbftest_06 rbftest_07 rbftest_08 rbftest_08b rbftest_09 rbftest_10 rbftest_11 rbftest_12 rbftest_update rbftest_delete

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

rbftest_01.o: pfm.h rbfm.h
rbftest_02.o: pfm.h rbfm.h
rbftest_03.o: pfm.h rbfm.h
rbftest_04.o: pfm.h rbfm.h
rbftest_05.o: pfm.h rbfm.h
rbftest_06.o: pfm.h rbfm.h
rbftest_07.o: pfm.h rbfm.h
rbftest_08.o: pfm.h rbfm.h
rbftest_08b.o: pfm.h rbfm.h
rbftest_09.o: pfm.h rbfm.h
rbftest_10.o: pfm.h rbfm.h
rbftest_11.o: pfm.h rbfm.h
rbftest_12.o: pfm.h rbfm.h
rbftest_update.o: pfm.h rbfm.h
rbftest_delete.o: pfm.h rbfm.h

# binary dependencies
rbftest_01: rbftest_01.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_02: rbftest_02.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_03: rbftest_03.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_04: rbftest_04.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_05: rbftest_05.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_06: rbftest_06.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_07: rbftest_07.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_08: rbftest_08.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_08b: rbftest_08b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_09: rbftest_09.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_10: rbftest_10.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_11: rbftest_11.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_12: rbftest_12.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_update: rbftest_update.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest_delete: rbftest_delete.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest_01 rbftest_02 rbftest_03 rbftest_04 rbftest_05 rbftest_06 rbftest_07 rbftest_08 rbftest_08b rbftest_09 rbftest_10 rbftest_11 rbftest_12 rbftest_update rbftest_delete *.a *.o *~

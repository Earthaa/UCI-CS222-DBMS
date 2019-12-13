include ../makefile.inc

all: librm.a rmtest_create_tables rmtest_delete_tables rmtest_00 rmtest_01 rmtest_02 rmtest_03 rmtest_04 rmtest_05 rmtest_06 rmtest_07 rmtest_08 rmtest_09 rmtest_10 rmtest_11 rmtest_12 rmtest_13 rmtest_13b rmtest_14 rmtest_15 rmtest_extra_1 rmtest_extra_2

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest_00.o: rm.h rm_test_util.h
rmtest_01.o: rm.h rm_test_util.h
rmtest_02.o: rm.h rm_test_util.h
rmtest_03.o: rm.h rm_test_util.h
rmtest_04.o: rm.h rm_test_util.h
rmtest_05.o: rm.h rm_test_util.h
rmtest_06.o: rm.h rm_test_util.h
rmtest_07.o: rm.h rm_test_util.h
rmtest_08.o: rm.h rm_test_util.h
rmtest_09.o: rm.h rm_test_util.h
rmtest_10.o: rm.h rm_test_util.h
rmtest_11.o: rm.h rm_test_util.h
rmtest_12.o: rm.h rm_test_util.h
rmtest_13.o: rm.h rm_test_util.h
rmtest_13b.o: rm.h rm_test_util.h
rmtest_14.o: rm.h rm_test_util.h
rmtest_15.o: rm.h rm_test_util.h
rmtest_extra_1.o: rm.h rm_test_util.h
rmtest_extra_2.o: rm.h rm_test_util.h
rmtest_create_tables.o: rm.h rm_test_util.h
rmtest_delete_tables.o: rm.h rm_test_util.h

# binary dependencies
rmtest_00: rmtest_00.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_01: rmtest_01.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_02: rmtest_02.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_03: rmtest_03.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_04: rmtest_04.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_05: rmtest_05.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_06: rmtest_06.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_07: rmtest_07.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_08: rmtest_08.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_09: rmtest_09.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_10: rmtest_10.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_11: rmtest_11.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_12: rmtest_12.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_13: rmtest_13.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_13b: rmtest_13b.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_14: rmtest_14.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_15: rmtest_15.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_extra_1: rmtest_extra_1.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_extra_2: rmtest_extra_2.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p0: rmtest_p0.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p1: rmtest_p1.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p2: rmtest_p2.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p3: rmtest_p3.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p4: rmtest_p4.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p5: rmtest_p5.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p6: rmtest_p6.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p7: rmtest_p7.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p8: rmtest_p8.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_p9: rmtest_p9.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_pex1: rmtest_pex1.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a
rmtest_pex2: rmtest_pex2.o librm.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a

.PHONY: clean
clean:
	-rm rmtest_create_tables rmtest_delete_tables rmtest_00 rmtest_01 rmtest_02 rmtest_03 rmtest_04 rmtest_05 rmtest_06 rmtest_07 rmtest_08 rmtest_09 rmtest_10 rmtest_11 rmtest_12 rmtest_13 rmtest_13b rmtest_14 rmtest_15 rmtest_extra_1 rmtest_extra_2 *.a *.o *~ tbl_* Tables Columns rids_file sizes_file

	$(MAKE) -C $(CODEROOT)/rbf clean

CXX=g++
CXXFLAGS= -g -Wall -Werror

# This can be multiple files
TARGET=main

TESTERS=m1_tester
DEPS=db index page table query Toolkit

DEPS_H=$(addsuffix .h,$(DEPS))
TESTERS_H=$(addsuffix .h,$(TESTERS))

DEPS_O=$(addsuffix .o,$(DEPS))
TESTERS_O=$(addsuffix .o,$(TESTERS))
TARGET_O=$(addsuffix .o,$(TARGET))

$(TARGET) : $(TARGET_O) $(DEPS_O) $(TESTERS_O)
	$(CXX) $(CXXFLAGS) $^ -o $@

# Assuming target will need everything
$(filter %.o,$(TARGET_O)): %.o : %.cpp $(DEPS_H) $(TESTERS_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Base files
db.o : db.cpp db.h table.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
index.o : index.cpp index.h table.h RID.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
page.o : page.cpp page.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
## Add deps once they are done
query.o : query.cpp query.h
	$(CXX) $(CXXFLAGS) -c $< -o $@
table.o : table.cpp table.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Toolkit
Toolkit.o : Toolkit.cpp Toolkit.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Assuming All testers need the same thing
$(filter %.o,$(TESTERS_O)): %.o : %.cpp %.h query.h db.h table.h toolkit.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

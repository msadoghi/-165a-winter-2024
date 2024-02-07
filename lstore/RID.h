#ifndef RIDH
#define RIDH
#include <vector>

// RID class contains everything associated with one record
// This includes RID id number, pointers to each page including data and metadata

class RID {
public:
    const int INDIRECTION_COLUMN = 0;
    const int RID_COLUMN = 1;
    const int TIMESTAMP_COLUMN = 2;
    const int SCHEMA_ENCODING_COLUMN = 3;
    RID ();
    virtual ~RID ();
    std::vector<int*> pointers;
    int id;
    RID (std::vector<int*> ptr, int i) : pointers(ptr), id(i) {};
};

#endif

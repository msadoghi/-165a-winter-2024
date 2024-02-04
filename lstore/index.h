#ifndef INDEXH
#define INDEXH
#include <unordered_map>
#include <vector>
#include "RID.h"

class Index {
private:
    /* data */
    Table table;
    std::unordered_map<int, std::unordered_multimap<int, std::vector<RID>>> indices;

public:
    Index (Table t) : table(t) {};
    virtual ~Index ();
    std::vector<RID> locate(int column_number, int value);
    std::vector<RID> locate_range(int begin, int end, int column_number);
    void create_index(int column_number);
    void drop_index(int column_number);
};

#endif

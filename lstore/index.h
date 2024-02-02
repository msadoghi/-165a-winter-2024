#ifndef INDEXH
#define INDEXH
#include <unordered_map>
#include <unordered_multimap>
#include <vector>
#include "RID.h"

class Index {
private:
    /* data */
    Table table;
    std::unordered_map<std::pair<int, std::unordered_multimap<int, std::vector<RID>>>> indices(table.num_columns);

public:
    Index (Table t) : table(t) {};
    virtual ~Index ();
    std::vector<std::vector<RID>> locate(int column, int value);
    std::vector<std::vector<RID>> locate_range(int begin, int end, int column);
    void create_index(int column_number);
    void drop_index(int column_number);
};

#endif


map<column, index>

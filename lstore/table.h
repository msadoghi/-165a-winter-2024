#ifndef TABLEH
#define TABLEH
# include <vector>
# include <map>
#include <utility>
#include <ctime>
#include <string>
#include "index.h"
#include "page.h"

// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Table {
    public:
    Table(std::string name, int key, int num_columns): name(name), key(key), num_columns(num_columns) {
        this->index = new Index();
        index->setTable(this);
    };
    friend class Index;

    private:
    std::string name;
    int key; //primary key
    int num_columns; //number of columns of actual data, excluding the metadata
    std::map<int, RID> page_directory; //<RID.id, RID>
    Index* index;
    int last_page = -1;
    std::vector<PageRange> pages;
    int num_update = 0;
    int num_insert = 0;

    void merge(){}
};

#endif

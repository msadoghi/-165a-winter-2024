#ifndef TABLEH
#define TABLEH
# include <vector>
# include <map>
#include <utility>
#include <ctime>
#include <string>
#include "index.h"
#include "page.h"

const int INDIRECTION_COLUMN = 0;
const int TIMESTAMP_COLUMN = 1;
const int SCHEMA_ENCODING_COLUMN = 2;

// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Record {
    public:
    Record(int rid, int key, std::vector<int> columns) : int(rid), key(key), columns(columns) {};

    private:
        int rid;
        int key;
        std::vector<int> columns;
};

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

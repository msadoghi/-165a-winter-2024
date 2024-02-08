#include "Table.h"

class Table;

Table::Table(std::string name, int key, int num_columns) : name{name}, key{key}, num_columns{num_columns} {
    Page* new_base_page = new Page[num_columns + 4];
    base_pages.push_back(new_base_page);
}

Table::~Table() {}

void Table::add_record(int64_t* input_data) {
    auto final_row = base_pages.back();
    for (int i = 0; i < num_columns; i++) {
        final_row[i].add_record(input_data[i]);
    }
}

/***
 *
 * A data structure indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
 *
 */

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_multimap>
#include "index.h"
#include "table.h"
#include "RID.h"

Index::Index(Table t) : table(t) {
    create_index(table.key);
}

/***
 *
 * returns the location of all records with the given value on column named "column"
 *
 * @param string column Name of column to look value for
 * @param int value Value to look for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate (int column, int value) {
    std::vector<RID> matching_records; //holds the records that match the value
    std::unordered_map<std::pair<int, std::unordered_multimap<int, std::vector<RID>>>>::iterator iter;
    iter = indices.find(column); //find index for specified column
    if(iter == std::map::end){
      throw std::invalid_argument("No index for that column was located.");
    }
    auto range = iter->equal_range(value); //check for all matching records in the index
    for(auto it = range.first; it != range.second; it++){
        matching_records.insert(matching_records.end(), it->second.begin(), it->second.end());
    }
    return matching_records;
}

/***
 *
 * returns the locations of all records with the given range on column named "column"
 *
 * @param int begin Lower bound of the range
 * @param int end Higher bound of the range
 * @param int column Name of column to look value for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate_range(int begin, int end, int column) {
    std::vector<RID> matching_records; //holds the records that match a value in the range
    std::vector<RID> all_matching_records //holds the matching records from the whole range
    for (int i = begin; i < end; i++) {
      matching_records = locate(column, i); //locate for each value in the range
      all_matching_records.insert(all_matching_records.end(), matching_records.begin(), matching_records.end());
    }
    return all_matching_records;
}

/***
 *
 * Create index on given column
 *
 * @param int column_number number of column to create index on
 *
 */
void Index::create_index(int column_number) {
    return;
}

/***
 *
 * Delete index on given column
 *
 * @param int column_number number of column to delete index on
 *
 */
void Index::drop_index(int column_number) {
    return;
}

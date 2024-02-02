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
 * returns the location of all records with the given value in the indexed column
 *
 * @param int column_number The indexed column to search through
 * @param int value Value to look for
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate (int column_number, int value) {
    std::vector<RID> matching_records; //holds the records that match the value
    auto index = indices.find(column_number); //find index for specified column
    if(index == std::map::end){
      throw std::invalid_argument("No index for that column was located.");
    }
    auto range = index->equal_range(value); //check for all matching records in the index
    for(auto iter = range.first; iter != range.second; iter++){
        matching_records.insert(matching_records.end(), iter->second.begin(), iter->second.end());
    }
    return matching_records;
}

/***
 *
 * returns the location of all records with the given value in the indexed column
 *
 * @param int begin Lower bound of the range
 * @param int end Higher bound of the range
 * @param int column_number The indexed column to search through
 *
 * @return Return one or more RIDs
 *
 */
std::vector<RID> Index::locate_range(int begin, int end, int column_number) {
    std::vector<RID> matching_records; //holds the records that match a value in the range
    std::vector<RID> all_matching_records //holds the matching records from the whole range
    for (int i = begin; i < end; i++) {
      matching_records = locate(column_number, i); //locate for each value in the range
      all_matching_records.insert(all_matching_records.end(), matching_records.begin(), matching_records.end());
    }
    return all_matching_records;
}

/***
 *
 * Create index on given column
 *
 * @param int column_number Which column to create index on
 *
 */
void Index::create_index(int column_number) {
    return;
}

/***
 *
 * Delete index on given column
 *
 * @param int column_number Which column to delete index of
 *
 */
void Index::drop_index(int column_number) {
  auto index = indices.find(column_number);
  if(index == std::map::end){
    throw std::invalid_argument("No index for that column was located. The index was not dropped.");
  }
  indices.erase(column_number);
  return;
}

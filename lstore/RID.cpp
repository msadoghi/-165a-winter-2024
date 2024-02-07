#include "RID.h"

bool RID::check_schema (int column_num) {
    int schema_encoding = *(pointers[SCHEMA_ENCODING_COLUMN]);
    int bin = 0b1 & (schema_encoding >> (column_num - 1));
    return bin;
}

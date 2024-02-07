#include "RID.h"

bool RID::check_schema (int column_num) {
    int schema_encoding = *(pointers[SCHEMA_ENCODING_COLUMN]);
    int bin = 0b1 & (schema_encoding >> (column_num - 1));
    return bin;
}

int RID::column_with_one () {
    int num_elements = pointers.size() - 3; //Number of metadata columns
    int schema_encoding = *(pointers[SCHEMA_ENCODING_COLUMN]);
    for (int i = 0; i < num_elements; i++) {
        if (0b1 & schema_encoding) {
            return num_elements - i;
        }
        schema_encoding = schema_encoding >> 1;
    }
    return -1;
}

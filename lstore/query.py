from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table = None):
        self.table = table
        self.index = Index(table)

    """
    # internal Method
    # Read a record with specified key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        rid = self.table.key_RID[key]
        del self.table.key_RID[key]
        del self.table.page_directory[rid]
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        key = columns[self.table.key_column]
        if key in self.table.key_RID.keys():
            return False
        
        # Acquire lock
        self.table.new_record.acquire()

        # Set metadata info
        schema_encoding = '0' * self.table.num_columns
        indirection = SPECIAL_NULL_VALUE
        rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        base_id = rid
        
        # Put in list
        meta_data = [indirection, rid, int(time), schema_encoding, base_id]
        columns = list(columns)
        # Add on data columns
        meta_data.extend(columns)
        # Write to base pages in table
        self.table.base_write(meta_data)

        # Release Lock
        self.table.new_record.release()
        return True
    
    # Find column bufferID in base page
    def find_column_bufferID(self, column):
        buffer_rid = []
        num_base = self.table.num_records - self.table.num_updates
        num_page_range = num_base % RECORDS_PER_PAGE_RANGE + 1
        num_base_page = num_base % RECORDS_PER_PAGE + 1
        # Iterate through page ranges
        for page_range in range(num_page_range):
            # Iterate through base pages in page ranges
            for base_page in range(num_base_page):
                buffer_id = (self.table.table_path, "base", str(column), str(page_range), str(base_page))
                buffer_rid.append(buffer_id) 
        
        # Return a list of buffer_RIDs that correspond to base pages
        return buffer_rid
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        records = []
        column = []
        rids = []

        # If search_key_index refers to key_column, add key_RID to rids
        if search_key_index == self.table.key_column:
            if search_key in self.table.key_RID.keys():
                rids.append(self.table.key_RID[search_key])
        # Elif table index has search_key_index, locate and set rids to
        elif self.table.index.has_index(search_key_index):
            rids = self.table.index.locate(search_key_index, search_key)
        # Else, find corresponding rids and set to rids
        else:
            rids = self.table.find_rids(search_key_index, search_key)

        # If can't find anything, return empty list
        if len(rids) == 0:
                return []
        
        # Iterate through found rids
        for rid in rids:
            result = self.table.find_record(rid)
            column = result[(META_COLUMN_COUNT + 1):(META_COLUMN_COUNT + 1) + self.table.num_columns + 1]
        
            # If record has update record
            if result[INDIRECTION_COLUMN] != SPECIAL_NULL_VALUE:
            # Use INDIRECTION_COLUMN of base record to find tail record
                rid_tail = result[INDIRECTION_COLUMN]
                result_tail = self.table.find_record(rid_tail)
                updated_column = result_tail[(META_COLUMN_COUNT + 1):(META_COLUMN_COUNT + 1) + self.table.num_columns + 1]

                # Grab schema encoding and findout if there was an updated column
                encoding = result[SCHEMA_ENCODING_COLUMN]
                encoding = self.find_changed_col(encoding)
                for i, value in enumerate(encoding):
                    if value == 1:
                        column[i] = updated_column[i]
        
            # Find columns that are requested
            for i in range(self.table.num_columns):
                # 0 indicates that column is not wanted, set to None
                if projected_columns_index[i] == 0:
                    column[i] = None
            # Add found record to records list
            record = Record(rid, search_key, column)
            records.append(record)
            
        return records

    # Helper function to find which columns have updated
    def find_changed_col(self, encoding):
        count = self.table.num_columns
        result = [0 for _ in range(count)]
        if encoding == 0:
            return result
        while encoding != 0:
            count -= 1
            if count < 0:
                break
            if encoding % 10 == 1:
                result[count] = 1

            encoding = encoding // 10
        return result
        
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # Turn given columns into a list
        columns = list(columns)

        # Check if given primary_key & RID in columns is in key_RID dict & exists
        if primary_key not in self.table.key_RID.keys():
            return False
        if columns[self.table.key_column] in self.table.key_RID.keys():
            return False
        if columns[self.table.key_column] != None:
            return False
        
        # Acquire lock
        self.table.update_record.acquire()

        # Grab metadata info
        tail_rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        rid = self.table.key_RID[primary_key]
        base_id = rid
        result = self.table.find_record(rid)
        indirection = result[INDIRECTION_COLUMN]
        new_encoding = '' 
        location_base = self.table.page_directory[rid]
        
        # First time update, need to set indirection & schema encoding
        if indirection == SPECIAL_NULL_VALUE:
            tail_indirect = rid
            for i, value in enumerate(columns):
                if value == None:
                    new_encoding += '0'
                    columns[i] = SPECIAL_NULL_VALUE
                else:
                    new_encoding += '1'
        # Otherwise
        else:
            # Grab indirection & schema encoding info
            latest_tail = self.table.find_record(indirection)
            tail_indirect = latest_tail[RID_COLUMN]
            encoding = latest_tail[SCHEMA_ENCODING_COLUMN]
            encoding = self.find_changed_col(encoding)

            # Check modified columns in schema encoding
            for i, value in enumerate(encoding):
                if columns[i] != None:
                    new_encoding += '1'
                else:
                    columns[i] = latest_tail[i + (META_COLUMN_COUNT + 1)]
                    if latest_tail[i + (META_COLUMN_COUNT + 1)] != SPECIAL_NULL_VALUE:
                        new_encoding += '1'
                    else:
                        new_encoding += '0'
        
        # Update base record
        self.table.update_value(INDIRECTION_COLUMN, location_base, tail_rid)
        self.table.update_value(SCHEMA_ENCODING_COLUMN, location_base, new_encoding)

        # Put metadata info into list
        meta_data = [tail_indirect, tail_rid, int(time), new_encoding, base_id]
        # Add on data columns
        meta_data.extend(columns)
        # Write to tail page
        self.table.tail_write(meta_data)

        # Release lock
        self.table.update_record.release()
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        column_index = aggregate_column_index + (META_COLUMN_COUNT + 1)
        # Iterate through given range
        for key in range(start_range, end_range + 1):
            if key in self.table.key_RID.keys():
                rid = self.table.key_RID[key]
                record = self.table.find_record(rid)
                # If never been updated, just grab
                if record[INDIRECTION_COLUMN] == SPECIAL_NULL_VALUE:
                    total_sum += record[column_index]

                # Otherwise, have to find newest version
                else:
                    tail_rid = record[INDIRECTION_COLUMN]
                    tail_record = self.table.find_record(tail_rid)
                    updated_column = tail_record[(META_COLUMN_COUNT + 1):(META_COLUMN_COUNT + 1) + self.table.num_columns + 1]
                    encoding = tail_record[SCHEMA_ENCODING_COLUMN]
                    encoding = self.find_changed_col(encoding)
                    if encoding[aggregate_column_index] == 1:
                        total_sum += updated_column[aggregate_column_index]
                    else:
                        total_sum += record[column_index]
        return total_sum

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key_column, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
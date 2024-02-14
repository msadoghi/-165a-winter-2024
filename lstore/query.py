from lstore.table import Table, Record
from lstore.index import Index
import math
import time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        base = self.find_base_record(primary_key, 0)
        record = self.get_record(base['pi'], base['rid'])
        self.delete_loop(base['pi'], record)

    def delete_loop(self, page_id, from_record):
        #if from_record == None:
            #return
        if from_record.indirection == -1:
            from_record.rid = -1
            return
        self.delete_loop(page_id, self.get_record(page_id, from_record.indirection))
        from_record.rid = -1

    def find_base_record(self, key, key_index):
        if self.table.index.indices[key_index] is not None:
            return self.table.index.locate(key_index, key)
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i][0]
            for j in range(len(page.data)):
                if j not in page.records or not self.check_record_validity(page.records[j], key, key_index):
                    continue
                return {'pi': i, 'rid': page.records[j].rid}
        return None

    def find_first_free_slot(self, replace=False):
        for pi in range(self.table.page_range):
            page = self.table.page_directory[pi][0]
            if not page.has_capacity():
                continue
            if not replace:
                return {'pi': pi, 'slot_index': page.num_records}
            for i in range(len(page.data)):
                if i in page.records and page.records[i].rid != -1:
                    continue
                return {'pi': pi, 'slot_index': i}
        return None

    def check_is_farthest(self, slot):
        if (slot['pi'] > self.table.farthest['pi']) or (slot['pi'] == self.table.farthest['pi'] and slot['slot_index'] > self.table.farthest['slot_index']):
            self.table.farthest = slot

    def check_record_validity(self, record, key, key_index):
        if record.rid == -1 or record.columns[key_index] != key:
            return False
        return True

    def check_range(self, start, end, v):
        if v < start or v > end:
            return False
        return True

    def get_record(self, page_id, rid):
        page_stack = self.table.page_directory[page_id]
        index = math.floor(rid / len(page_stack[0].data))
        slot_index = rid % len(page_stack[0].data)
        return page_stack[index].records[slot_index]

    def get_latest_version_rid(self, page_id, from_rid):
        indirection = self.get_record(page_id, from_rid).indirection
        if indirection == -1:
            return from_rid
        return self.get_latest_version_rid(page_id, indirection)

    def get_version_count(self, page_id, from_rid, count=1):
        indirection = self.get_record(page_id, from_rid).indirection
        if indirection == -1:
            return count
        return self.get_version_count(page_id, indirection, count + 1)

    def get_relative_version_rid(self, page_id, base_rid, relative_version):
        counter = self.get_version_count(page_id, base_rid) + relative_version
        return self.relative_version_rid_loop(page_id, base_rid, counter)

    def relative_version_rid_loop(self, page_id, from_rid, counter):
        indirection = self.get_record(page_id, from_rid).indirection
        if indirection == -1 or counter <= 1:
            return from_rid
        return self.relative_version_rid_loop(page_id, indirection, counter - 1)

    def get_values_from_range(self, start_range, end_range, key_index, search_index, relative_version=0):
        result = []
        if self.table.index.indices[key_index] is not None:
            info = self.table.index.locate_range(start_range, end_range, key_index)
            for base in info:
                result.append(self.get_record(base['pi'], self.get_relative_version_rid(base['pi'], base['rid'], relative_version)).columns[search_index])
            return result
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i][0]
            for j in range(len(page.data)):
                if j not in page.records or page.records[j].rid == -1:
                    continue
                record = self.get_record(i, self.get_relative_version_rid(i, page.records[j].rid, relative_version))
                if not self.check_range(start_range, end_range, record.columns[key_index]):
                    continue
                result.append(record.columns[search_index])
        return result


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        slot = self.find_first_free_slot()
        self.check_is_farthest(slot)
        page = self.table.page_directory[slot['pi']][0]
        rid = slot['slot_index']
        new_record = Record(rid, 0, columns)
        new_record.schema_encoding = '0' * self.table.num_columns
        page.write(new_record)
        for i in range(len(columns)):
            if self.table.index.indices[i] is not None:
                self.table.index.indices[i].insert_record(columns[i], rid, slot['pi'])
        return True

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
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

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
        base = self.find_base_record(search_key, search_key_index)
        selected = [self.get_record(base['pi'], self.get_relative_version_rid(base['pi'], base['rid'], relative_version))]
        return selected


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        base = self.find_base_record(primary_key, 0)
        last_rid = self.get_latest_version_rid(base['pi'], base['rid'])
        last_record = self.get_record(base['pi'], last_rid)
        page_stack = self.table.page_directory[base['pi']]
        index = page_stack['size'] - 1
        tail = page_stack[index]
        if index == 0 or not tail.has_capacity():
            tail = self.table.add_tail(base['pi'])
            index += 1
        rid = index * len(page_stack[0].data) + tail.num_records
        last_record.indirection = rid
        new_columns = []
        for k, v in enumerate(columns):
            if v is None:
                new_columns.append(last_record.columns[k])
            else:
                new_columns.append(v)
        new_record = Record(rid, 0, new_columns)
        tail.write(new_record)
        return True

        last_rid = -1
        for i in range(self.table.farthest['pi'] + 1):
            page_stack = self.table.page_directory[i]
            page = page_stack[0]
            for j in range(len(page.data)):
                if j not in page.records or not self.check_record_validity(page.records[j], primary_key, page.records[j].key):
                    continue
                last_rid = page.records[j].rid
                break
            if last_rid != -1:
                last_record = self.get_record(i, self.get_latest_version_rid(i, last_rid))
                index = page_stack['size'] - 1
                tail = page_stack[index]
                if index == 0 or not tail.has_capacity():
                    tail = self.table.add_tail(i)
                    index += 1
                rid = index * len(page.data) + tail.num_records
                last_record.indirection = rid
                new_columns = []
                for k, v in enumerate(columns):
                    if v is None:
                        new_columns.append(last_record.columns[k])
                    else:
                        new_columns.append(v)
                new_record = Record(rid, 0, new_columns)
                tail.write(new_record)
                return True
        return False



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)


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
        count = 0
        a = self.get_values_from_range(start_range, end_range, 0, aggregate_column_index, relative_version)
        for i in a:
            count += i
        return count

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

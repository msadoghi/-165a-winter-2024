import random
import math

"""
Internal node object used for quickly traversing values stored in the index.
"""
class IndexNode:
    def __init__(self, in_value, in_rid):
        self.value = in_value
        self.rid = [in_rid]
        self.next = None

"""
Storage class for each column for indexing purposes.
"""
class IndexStore:

    def __init__(self):
        self.stored_records = {}
        self.first_node = None
        self.maximum_value = None
        self.sorted_seeds = []
    
    # Finds the largest key that is smaller than the desired value.
    def find_largest_smaller_key(self, desired_value):

        # To handle potential edge cases where the sorted seeds give nothing.
        desired_node = self.first_node

        # Parse through the sorted seeds list.
        for current_seed in self.sorted_seeds:
            if current_seed.value < desired_value:
                desired_node = current_seed

        # Parse for the node exactly before the closest value.
        while desired_node is not None and desired_node.next is not None:
            if desired_node.next.main_key >= desired_value:
                break
            desired_node = desired_node.next
        
        return desired_node

    def make_seeds(self):
        count = math.ceil(len(self.stored_records) / 100)
        seeds_i = random.sample(range(0, len(self.stored_records)), count)
        self.sorted_seeds.clear()
        for i in seeds_i:
            self.sorted_seeds.append(self.stored_records[i])
        self.sorted_seeds.sort(key=lambda value: value)

    def insert_record(self, in_value, in_rid):

        if in_value in self.stored_records:
            self.stored_records[in_value].rid.append(in_rid)
            return

        if (self.maximum_value is None) or (self.maximum_value < in_value):
            self.maximum_value = in_value

        new_node = IndexNode(in_value, in_rid)
        if self.first_node is None:
            # There is no first node, making this the first.
            self.first_node = new_node
        else:
            if self.first_node.value > in_value:
                # the new node is smaller than the current smallest.
                new_node.next = self.first_node
                self.first_node = new_node
            else:
                # insert the record's node into the list of nodes.
                new_child_node = self.find_largest_smaller_key(in_value)
                new_node.next = new_child_node.next
                new_child_node.next = new_node

        # Hash the rid into its storage location in the hash table
        self.stored_records[in_value].append(new_node)

        if len(self.stored_records) % 100 == 0:
            self.make_seeds()


"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # store the table.
        self.table_ref = table

        # One index for each table. All our empty initially.
        self.indices = [[]] * table.num_columns

        # initialize each column in the index storage.
        self.create_index(0)


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        
        # All data that maps to value should hash to indices[column]
        return self.indices[column].stored_records[value][:]
        
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        
        foundRIDs = []

        col_data = self.indices[column]
        desired_node = col_data.find_largest_smaller_key(begin)
        while desired_node is not None and desired_node.next is not None and desired_node.next.value <= begin:
            desired_node = desired_node.next

        while desired_node is not None and desired_node.next is not None and desired_node.next.value <= end:
            # the first node found is actually the one smaller than begin.
            desired_node = desired_node.next
            for rid in desired_node.rid:
                foundRIDs.append(rid)

        return foundRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        created_store = IndexStore()
        self.indices[column_number] = created_store

        # catlogue the indices of each individual record.
        for i in range(self.table_ref.farthest['pi'] + 1):
            page = self.table_ref.page_directory[i]
            for j in range(self.table_ref.farthest['slot_index'] + 1):
                record = page.records[j]
                created_store.insert_record(record.columns[column_number], record.rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number].clear()

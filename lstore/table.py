from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from lstore.bufferpool import BufferPool
from time import time

# One record has a Page per column
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = -1
        self.schema_encoding = []

    def get_key(self):
        return self.columns[self.key]

# One pagerange tracks a set of base pages & tail pages
class PageRange:

    def __init__(self):
        self.base_page_index = 0 
        self.tail_page_index = 0 
        self.base_page = [None for _ in range(BASE_PAGE_MAX)]
        self.tail_page = [None] 
        
    def is_page_exist(self, index, type):
        if type == "base":
            return self.base_page[index] != None
        else:
            return self.tail_page[index] != None
        
    # content will be a page object(read from disk) if passed in    
    def create_base_page(self, index, content = None): 
        if content == None:
            self.base_page[index] = Page()
        else:
            self.base_page[index] = content

    def inc_base_page_index(self):
        self.base_page_index += 1

    def current_base_page(self):
        return self.base_page[self.base_page_index]

    def current_tail_page(self):
        return self.tail_page[self.tail_page_index]

    def add_tail_page(self):
        if self.tail_page[self.tail_page_index] == None:
            self.tail_page[self.tail_page_index] = Page()
        else:
            self.tail_page.append(Page())
            self.tail_page_index += 1

    def last_base_page(self):
        return self.base_page_index == BASE_PAGE_MAX - 1

# Table keeps track of pageranges
class Table:

    """
    :param name: string         #Table name
    :param key: int             #Index of table key in columns
    :param num_columns: int     #Number of Columns: all columns are integer
    :param page_directory: dict #Page directory
    :param num_page_ranges: int #Number of page ranges
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.page_range = BASE_PAGE_MAX
        self.tail_pages = []
        self.farthest = {'pi': 0, 'slot_index': -1}
        for i in range(self.page_range):
            self.page_directory[i] = {}
            page_stack = self.page_directory[i]
            page_stack[0] = Page()
            page_stack[0].id = i
            page_stack['size'] = 1
        self.index = Index(self)

    def add_tail(self, pi):
        tail = Page()
        page_stack = self.page_directory[pi]
        tail.id = page_stack[0].id
        page_stack[page_stack['size']] = tail
        page_stack['size'] += 1
        return tail

    def __merge(self):
        print("merge is happening")
        pass
 
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key_column):
        self.table_path = ""
        self.name = name
        self.key_column = key_column
        self.num_columns = num_columns
        self.total_num_columns = self.num_columns+(META_COLUMN_COUNT + 1)
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.num_updates = 0
        self.key_RID = {}
        
        '''
        # Key: (table_name, col_index, page_range_index), value: threading lock 
        self.rid_locks = defaultdict(lambda: defaultdict(threading.Lock())) 
        # Key: (table_name, col_index, page_range_index,page_index), value: threading lock  
        self.page_locks = defaultdict(lambda: defaultdict(threading.Lock()))
        '''
        
    def get_rid(self, key):
        return self.key_RID[key]

    # using a number to find the location of the page 
    def determine_page_location(self, type):
        num_tail = self.num_updates
        num_base = self.num_records - num_tail
        page_range_index = num_base % RECORDS_PER_PAGE_RANGE
        if type == 'base':
            base_page_index = num_base % RECORDS_PER_PAGE
            return page_range_index, base_page_index
        else:
            tail_page_index = num_tail % RECORDS_PER_PAGE
            return page_range_index, tail_page_index

    # column is the insert data
    def base_write(self, columns):
        # self.new_record.acquire()
        page_range_index, base_page_index = self.determine_page_location('base')
        for i, value in enumerate(columns):     
            # use buffer_id to find the page
            buffer_id = (self.name, "base", i, page_range_index, base_page_index)
            page = BufferPool.get_page(buffer_id)
            
            if not page.has_capacity():
                if base_page_index == BASE_PAGE_MAX - 1:
                    page_range_index += 1
                    base_page_index = 0
                else:
                    base_page_index += 1
                buffer_id = (self.name, "base", i, page_range_index, base_page_index)
            
            
            # write in
            page.write(value)
            offset = page.num_records - 1
            BufferPool.updata_pool(buffer_id, page)  
        
        # write address into page directory
        rid = columns[0]
        address = [self.name, "base", page_range_index, base_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + (META_COLUMN_COUNT + 1)]] = rid
        self.num_records += 1
        self.index.push_index(columns[(META_COLUMN_COUNT + 1):len(columns) + 1], rid)
        # self.new_record.release()
        

    def tail_write(self, columns):
        # self.update_record.acquire()
        page_range_index, tail_page_index = self.determine_page_location('tail')
        # print("column in baseWrite: {}".format(column))
        for i, value in enumerate(columns):
            # use buffer_id to find the page
            buffer_id = (self.name, "tail", i, page_range_index,tail_page_index)
            page = BufferPool.get_page(buffer_id)
            
            if not page.has_capacity():
                tail_page_index += 1
                buffer_id = (self.name, "tail", i, page_range_index,tail_page_index)
                    
            page = BufferPool.get_page(buffer_id)
            # write in
            # print("value in tail_write: {} {}".format(i, value))
            page.write(value)
            offset = page.num_records - 1
            BufferPool.updata_pool(buffer_id, page)
            
        # write address into page directory
        rid = columns[0]
        address = [self.name, "tail", page_range_index, tail_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + (META_COLUMN_COUNT + 1)]] = rid
        self.num_records += 1
        self.num_updates += 1
        # self.update_record.release()
        
    def find_value_rid(self, column_index, target):
        rids = []
        for rid in self.page_directory:
            record = self.find_record(rid)
            if record[column_index + (META_COLUMN_COUNT + 1)] == target:
                rids.append(rid)
        return rids

    # pages is the given column that we are going to find the sid
    def find_value(self, column_index, location):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        value = page.get_value(location[4])
        return value

    def update_value(self, column_index, location, value):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        page.update(location[4], value)
        BufferPool.updata_pool(buffer_id, page)

    def find_record(self, rid):
        row = []
        location = self.page_directory[rid]
        for i in range((META_COLUMN_COUNT + 1) + self.num_columns):
            result = self.find_value(i, location)
            row.append(result)
        return row
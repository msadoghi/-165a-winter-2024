from lstore.index import Index
from lstore.page import Page
from lstore.config import *
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
 

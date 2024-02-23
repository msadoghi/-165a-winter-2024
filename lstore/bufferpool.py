from datetime import datetime
from lstore.page import Page
from lstore.config import *
import numpy as np
import pickle
import os

# BufferPool class
# Creates a "pool" of memory pages to read and write data from/to disk storage
class BufferPool:
    path = ""                     # Path to storage
    pool = {}                     # "pool" in the form of a dict
    capacity = BUFFERPOOL_SIZE    # Total pool capacity

    # 
    def __init__(self, capacity = BUFFERPOOL_SIZE):
        self.capcity = capacity
        pass

    # Class methods we don't care about a class instance(object), just the class methods themselves

    # Set initial path
    @classmethod 
    def initial_path(cls, path):
        cls.path = path

    # Check if buffer_id is in pool
    @classmethod 
    def page_buffer_checker(cls, buffer_id):
        return buffer_id in cls.pool.keys()
    
    # Add a page to the pool
    @classmethod  
    def add_pages(cls, buffer_id, page):
        cls.pool[buffer_id] = page
        cls.pool[buffer_id].set_dirty()

    @classmethod 
    def bufferid_path_pkl(cls, buffer_id):
        dirname = os.path.join(cls.path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        file_path = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        return file_path
    
    # Retrieve a page from the pool using a given buffer_id
    @classmethod 
    def get_page(cls, buffer_id):
        # If in pool, return
        if buffer_id in cls.pool:
            return cls.pool[buffer_id]
        path = cls.bufferid_path_pkl(buffer_id)
        # If not in disk, create new_page
        if not os.path.isfile(path):
            page = Page()
            cls.add_pages(buffer_id, page)
            return page
        # If page already exist, grab from disk
        else:
            if not cls.page_buffer_checker(buffer_id):
                cls.pool[buffer_id] = cls.read_page(path)
            return cls.pool[buffer_id]
        
    # Read a page from disk
    @classmethod 
    def read_page(cls, path):
        f = open(path, 'r+b')
        page = Page()
        metadata = pickle.load(f)
        
        page.num_records = metadata[0]
        page.dirty = metadata[1]
        page.pinned = metadata[2]
        page.tps = metadata[3]
        page.data = pickle.load(f)
        f.close()
        return page
    
    # Write a page from pool to disk
    @classmethod  
    def write_page(cls, page, buffer_id):
        dirname = os.path.join(cls.path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        file_path = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(file_path, 'w+b')
        metadata = [page.num_records, page.dirty, page.pinned, page.tps]
        pickle.dump(metadata, f)
        pickle.dump(page.data, f)
        f.close()

    # Write everything from pool into disk, empty the pool
    @classmethod 
    def close(cls):
        for buffer_id in cls.pool:
            cls.write_page(cls.pool[buffer_id], buffer_id)


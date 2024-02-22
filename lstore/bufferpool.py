from datetime import datetime
from lstore.page import Page
from lstore.config import *
import numpy as np
import pickle
import os

class BufferPool:
    path = ""
    pool = {}
    capacity = 0
    
    def __init__(cls, capacity = 2000):
        cls.capcity = capacity
        pass
        
    @classmethod 
    def initial_path(cls, path):
        cls.path = path
    
    @classmethod 
    def page_buffer_checker(cls, buffer_id):
        return buffer_id in cls.pool.keys()
           
    @classmethod  
    def add_pages(cls, buffer_id, page):
        cls.pool[buffer_id] = page
        cls.pool[buffer_id].set_dirty()
        
    @classmethod 
    def updata_pool(cls, buffer_id, page):
        cls.pool[buffer_id] = page
        cls.pool[buffer_id].set_dirty()

    @classmethod 
    def is_full(cls):
        return cls.LRU.is_full()

    @classmethod 
    def bufferid_path_pkl(cls, buffer_id):
        dirname = os.path.join(cls.path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        file_path = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        return file_path
    

    @classmethod 
    def get_page(cls, buffer_id):
        if buffer_id in cls.pool:
            return cls.pool[buffer_id]
        path = cls.bufferid_path_pkl(buffer_id)
        # create new_page
        if not os.path.isfile(path):
            page = Page()
            cls.add_pages(buffer_id, page)
            return page
        # page already exist
        else:
            if not cls.page_buffer_checker(buffer_id):
                cls.pool[buffer_id] = cls.read_page(path)
            return cls.pool[buffer_id]
        
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

    @classmethod 
    def close(cls):
        for buffer_id in cls.pool:
            cls.write_page(cls.pool[buffer_id], buffer_id)


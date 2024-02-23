from lstore.config import *

# Page class
class Page:
    def __init__(self):
        self.num_records = 0
        self.dirty = False
        self.data = bytearray(PAGE_SIZE)
        self.tps = 0
        self.pinned = 0
    
    # Check if there's room
    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE
    
    # Write value to page
    def write(self, value):
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1

    def set_dirty(self):
        self.dirty = True

    # Get value from page
    def get_value(self, index):
        value = int.from_bytes(self.data[index * 8:(index + 1) * 8], 'big')
        return value
    
    # Find a value in page
    def find_value(self, target):
        offsets = []
        # Iterate through records in page
        for i in range(RECORDS_PER_PAGE):
            value = int.from_bytes(self.data[i * 8:(i + 1) * 8], 'big')
            if value == target:
                offsets.append(i)
        return offsets        
    
    # Update page
    def update(self, index, value):
        self.data[index * 8:(index + 1) * 8] = int(value).to_bytes(8, byteorder='big')
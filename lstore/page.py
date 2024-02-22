from lstore.config import *

# Base/Tail pages are a logical concept
# Base/Tail pages consist of multiple physical pages
# class Page:

#     def __init__(self):
#         self.num_records = 0
#         self.data = bytearray(PAGE_SIZE)
#         self.records = {}
#         self.id = -1

#     def has_capacity(self):
#         return len(self.data) - self.num_records > 0

#     def write(self, value):
#         if self.has_capacity():
#             index = self.num_records
#             self.records[index] = value
#             self.num_records += 1

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        # print("value in Page is: {}".format(value))
        # new_value=int(value)
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1

    def get_value(self, index):
        value = int.from_bytes(self.data[index * 8:(index + 1) * 8], 'big')
        return value
    
    def find_value(self, target):
        offsets = []
        for i in range(RECORDS_PER_PAGE):
            value = int.from_bytes(self.data[i * 8:(i + 1) * 8], 'big')
            if value == target:
                offsets.append(i)
        return offsets        

    def update(self, index, value):
        self.data[index * 8:(index + 1) * 8] = int(value).to_bytes(8, byteorder='big')
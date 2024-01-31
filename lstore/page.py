
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = []
    

    def has_capacity(self):
        if (self.num_records * 28) < 4096:
            return True
        else:
            return False

    def write(self, value):
        self.num_records += 1
        self.data.append(value)
        
    def __str__(self):
        ret = ""
        for i in self.data:
            ret += str(i)
        return ret

#ifndef RIDH
#define RIDH

// RID will hold page range, page number, slot number for each column in the record

class RID {
public:
    RID ();
    virtual ~RID ();
    std::vector<int> page_range;
    std::vector<int> page;
    std::vector<int> slot;
    int id;
    RID (std::vector<int> pr, std::vector<int> p, std::vector<int> s, int i) : page_range(pr), page(p), slot(s), id(i) {};
};

#endif

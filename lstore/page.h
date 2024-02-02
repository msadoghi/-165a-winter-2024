#ifndef PAGEH
#define PAGEH

class PageRange {
private:
    /* data */
    std::vector<Page> pages;
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    const int PAGE_RANGE_SIZE = 65536;


public:
    PageRange (int num_pages) {};
    virtual ~PageRange ();
    bool has_capacity ();
};

class Page {
private:
    /* data */
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096; // bytes
    const int INT_SIZE = 4; // bytes
    int SLOT_NUM = PAGE_SIZE/4; // bytes
    int num_records = 0;
    int* data = nullptr; // Data location(pointer)
    std::array<int, SLOT_NUM> availability; // 0 is empty, 1 is occupied, 2 is deleted.

public:
    Page ();
    virtual ~Page () {
        delete data[];
    }
    bool has_capacity();
    void write(int value);
    friend ostream& operator<<(ostream& os, const Page& p);

};

#endif

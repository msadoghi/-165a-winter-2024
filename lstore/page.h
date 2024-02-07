#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>

class Page {
private:
    /* data */
    /// @TODO Move this to config file
    constexpr static int PAGE_SIZE = 4096; // bytes
    constexpr static int NUM_SLOTS = PAGE_SIZE/sizeof(int); // bytes
    int num_rows = 0;
    int* data = nullptr; // Data location(pointer)
    int availability[NUM_SLOTS] = {0}; // 0 is empty, 1 is occupied, 2 is deleted.

public:
    Page ();
    virtual ~Page ();
    bool has_capacity();
    int* write(int value);
    friend std::ostream& operator<<(std::ostream& os, const Page& p);

};

class PageRange {
private:
    /* data */
    std::vector<Page*> page_range;
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    const int PAGE_RANGE_SIZE = 65536; // Do we need this?
    const int NUM_PAGES = PAGE_RANGE_SIZE / PAGE_SIZE;


public:
    PageRange ();
    virtual ~PageRange ();
    bool has_capacity ();
};

#endif

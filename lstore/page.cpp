#include <vector>
#include <array>
#include <iostream>
#include <cstdlib>
#include "page.h"

PageRange::PageRange (int num_pages) {
    for (int i = 0; i < num_pages; i++) {
        pages.push_back(new Page());
    }
}

/***
 *
 * Return if any page in the page range has capacity left or not.
 *
 * @return True if all pages has capacity left, False if not
 *
 */
bool PageRange::has_capacity () {
    for (std::vector<Page>::iterator itr = pages.begin(); itr != pages.end(); itr++) {
        if (!(*itr.has_capacity())) {
            return False;
        }
    }
    return True;
}

Page::Page() {
    data = (int*)malloc(PAGE_SIZE); //malloc takes number of bytes
}

Page::~Page() {
    free(data);
}

/***
 *
 * Return if the page has capacity left or not.
 *
 * @return True if page has capacity left, False if not
 *
 */
bool Page::has_capacity() {
    return(num_records * INT_SIZE < PAGE_SIZE);
}

/***
 *
 * Write value into page
 *
 * @param int value Value to write into
 *
 */
void Page::write(int value) {
    num_records++;
    if (!has_capacity) {
        // Page is full, add the data to new page
    }
    for (int loc = 0; loc < SLOT_NUM; loc++) {
        if (availability[i] == 0) {
            //insert on loc
            int offset = loc * INT_SIZE; // Bytes from top of the page
            int* insert = data + offset;
            *insert = value;
        }
    }
    // Write value in data somehow.
    return;
}

/***
 *
 * Print out content of Page as one long string into cout.
 *
 * @param ostream os Standard io
 * @param Page& p Page to print out
 * @return Standard io
 *
 */
ostream& operator<<(ostream& os, const Page& p)
{
    for (int i = 0; i < p.SLOT_NUM; i++) {
        os << *(p.data + i);
    }
    return os;
}

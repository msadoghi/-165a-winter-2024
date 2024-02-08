#ifndef TOOLKIT_H_
#define TOOLKIT_H_

#include <vector>
#include <string>


/*
 * Convenience methods for this library
 */
namespace Toolkit {

/*
 * Takes unique sampling of data.
 *
 * Params: data: data to read
 *		   sz: the sample size
 *
  *Returns: The sample
 */
std::vector<int> sample(std::vector<int> data, int sz);

std::string printArray(std::vector<int>& data);

}



#endif
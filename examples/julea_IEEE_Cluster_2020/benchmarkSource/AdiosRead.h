#ifndef ADIOS2_READ_H_
#define ADIOS2_READ_H_
#include <iostream>
#include <chrono>

// using Clock = std::chrono::steady_clock;
// using std::chrono::time_point;
// using std::chrono::duration_cast;
// using std::chrono::milliseconds;

void AdiosReadMinMax(std::string fileName, std::string variableName);
void AdiosRead(std::string engineName, std::string directory, size_t fileCount, uint32_t percentageVarsToRead);

// std::vector<time_point<Clock>> getBlockTimes;
// milliseconds blockDelta;
// milliseconds getDelta;
// std::vector<milliseconds> getBlockDelta;
// std::vector<milliseconds> getsDelta;

#endif /* ADIOS2_READ_H_ */

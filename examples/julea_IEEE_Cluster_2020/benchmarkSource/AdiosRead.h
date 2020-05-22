#ifndef ADIOS2_READ_H_
#define ADIOS2_READ_H_
#include <chrono>
#include <iostream>
#include <vector>

// using Clock = std::chrono::steady_clock;
// using std::chrono::time_point;
// using std::chrono::duration_cast;
// using std::chrono::milliseconds;

void AdiosReadMinMax(std::string engineName, std::string path, size_t fileCount,
                     size_t variablesToRead, size_t compareValue, bool useMin);
void AdiosRead(std::string engineName, std::string directory, size_t fileCount,
               uint32_t percentageVarsToRead);

void buildDebugFileName(std::string &fileName, std::string engineName,
                        std::string path, size_t filesToRead,
                        uint32_t variablesToRead, std::time_t &curr_time);
void printDebugHeader(std::ofstream &outputFile, std::time_t curr_time);

void readDirectory(const std::string &path, std::vector<std::string> &v,
                   std::ofstream &outputFile);

void readInput(const std::string &path, std::vector<std::string> &files,
               std::ofstream &outputFile);

void calculateMeanTime(std::ofstream &outputFile,
                       std::vector<std::chrono::milliseconds> &delta,
                       bool allBlocks);

// std::vector<time_point<Clock>> getBlockTimes;
// milliseconds blockDelta;
// milliseconds getDelta;
// std::vector<milliseconds> getBlockDelta;
// std::vector<milliseconds> getsDelta;

#endif /* ADIOS2_READ_H_ */

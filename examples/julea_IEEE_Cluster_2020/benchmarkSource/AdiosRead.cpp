/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application demonstrating some of the query possibilities enabled by the
 * JULEA database engine.
 *
 *  Created on: May 06, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include "AdiosRead.h"
#include <adios2.h>
#include <chrono>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;

void buildDebugFileName(std::string &fileName, std::string engineName,
                        std::string path, size_t filesToRead,
                        uint32_t variablesToRead, std::time_t &curr_time)
{
    std::ofstream outputFile;
    auto currentTime = std::chrono::system_clock::now();
    curr_time = std::chrono::system_clock::to_time_t(currentTime);

    char timeBuffer[80];
    std::tm *timeinfo;
    timeinfo = localtime(&curr_time);
    strftime(timeBuffer, 80, "-%Y-%m-%d-%I:%M%p", timeinfo);

    fileName = engineName + "-" + path + "-" + std::to_string(filesToRead) +
               "-" + std::to_string(variablesToRead) + timeBuffer + ".txt";
}

void printDebugHeader(std::ofstream &outputFile, std::time_t curr_time)
{
    outputFile << "--- AdiosRead ---" << std::endl;
    outputFile << "Current time: " << std::ctime(&curr_time);
    outputFile << "\nvariableName \n"
               << "BlkCnt: \tBlock count \n"
               << "Block:  \tAverage time to read block in ms \n"
               << "AllBl:  \tTime to read all blocks in ms \n\n"
               << "Step:   \tTime for a step in ms\n"
               << "SumIO:  \tTime for complete I/O in ms\n"
               << std::endl;
    outputFile << "-------------------------------" << std::endl;
}

/**
 * Read the passed directory.
 * Ignores . and ..
 * Checks for .dir directories from bp3 file fomat.
 * These cannot be read directly by BP3 (nor by any other engine).
 * BP3 needs only the .bp file as an input.
 * BP4 needs only the .bp directory as an input.
 * @param path       directory path
 * @param v          stores all file paths insides
 * @param outputFile file to write debug output to
 */
void readDirectory(const std::string &path, std::vector<std::string> &v,
                   std::ofstream &outputFile)
{
    DIR *dirPtr = opendir(path.c_str());
    struct dirent *dirEntry;
    std::string completeFileName;

    while ((dirEntry = readdir(dirPtr)) != NULL)
    {
        std::string file = dirEntry->d_name;
        size_t slen = file.length();
        if ((strcmp(dirEntry->d_name, ".") == 0) ||
            (strcmp(dirEntry->d_name, "..") == 0))
        {
            continue;
        }
        else if (slen >= 4 && file.compare(slen - 4, 4, ".dir") == 0)
        {
            outputFile << "something.dir that is ignored" << std::endl;
            continue;
        }

        completeFileName = path + "/" + dirEntry->d_name;
        v.push_back(completeFileName);

        outputFile << completeFileName << std::endl;
    }
    closedir(dirPtr);
}

/**
 * Reads the Input.
 * Checks whether the path is a file or directory (or anything else)
 * @param path       passed path
 * @param files      stores all file paths insides
 * @param outputFile file to write debug output to
 */
void readInput(const std::string &path, std::vector<std::string> &files,
               std::ofstream &outputFile)
{

    struct stat s;
    if (stat(path.c_str(), &s) == 0)
    {
        if (s.st_mode & S_IFDIR)
        {
            // it's a directory
            outputFile << "Passed directory contains: " << std::endl;
            readDirectory(path, files, outputFile);
        }
        else if (s.st_mode & S_IFREG)
        {
            // it's a file
            files.push_back(path);
        }
        else
        {
            // something else
            std::cout << "When reading path it was neither directory nor file. "
                      << std::endl;
        }
    }
    else
    {
        // error
        std::cerr << "Reading directory failed!" << std::endl;
    }
    outputFile << "-------------------------------" << std::endl;
}

void AdiosReadMinMax(std::string path, std::string variableName)
{
    std::cout << "AdiosReadMinMax" << std::endl;
}

/**
 * Calculate the mean time it takes to read one block.
 * @param outputFile file to write debug output to
 */
void caculateMeanBlockTime(std::ofstream &outputFile)
{
    // size_t sumTimes = 0;
    // size_t mean = 0;
    // for (auto &times : getBlockDelta)
    // {
    //     sumTimes += times.count();
    //     // std::cout << "getBlockDelta: " << times.count() << std::endl;
    // }
    // mean = (sumTimes / getBlockDelta.size());
    // // std::cout << "getBlockDelta.size(): " << getBlockDelta.size() <<
    // // std::endl; std::cout << "sumTimes: " << sumTimes << std::endl;
    // outputFile << "Block \t" << mean << std::endl;
}

/**
 * Calculate the mean time it takes to read all blocks.
 * @param outputFile file to write debug output to
 */
void caculateMeanGetsTime(std::ofstream &outputFile)
{
    // size_t sumTimes = 0;
    // size_t mean = 0;
    // for (auto &times : getsDelta)
    // {
    //     sumTimes += times.count();
    //     // std::cout << "getsDelta: " << times.count() << std::endl;
    // }
    // mean = (sumTimes / getsDelta.size());
    // // std::cout << "sumTimes: " << sumTimes << std::endl;
    // // std::cout << "getsDelta.size(): " << getsDelta.size() << std::endl;
    // outputFile << "AllBl \t" << mean << std::endl;
}

void calculateMeanTime(std::ofstream &outputFile,
                       std::vector<milliseconds> &delta, bool allBlocks)
{
    size_t sumTimes = 0;
    size_t mean = 0;
    for (auto &times : delta)
    {
        sumTimes += times.count();
        // std::cout << "delta: " << times.count() << std::endl;
    }
    mean = (sumTimes / delta.size());
    // std::cout << "sumTimes: " << sumTimes << std::endl;
    // std::cout << "getsDelta.size(): " << delta.size() << std::endl;
    if (allBlocks)
    {
        // std::cout << "AllBl \t" << mean << std::endl;
        outputFile << "AllBl \t" << mean << std::endl;
    }
    else
    {
        // std::cout << "Block \t" << mean << std::endl;
        outputFile << "Block \t" << mean << std::endl;
    }
}

/**
 * Calculate the time interval for a step and the complete I/O
 * @param outputFile file to write debug output to
 */
void calculateIOTime(std::ofstream &outputFile)
{
    // milliseconds timeOpenClose =
    //     duration_cast<milliseconds>(endOpen - startOpen);
    // milliseconds timeStep = duration_cast<milliseconds>(endStep - startStep);

    // outputFile << "\nStep \t" << timeStep.count() << std::endl;
    // outputFile << "SumIO \t" << timeOpenClose.count() << std::endl;
    // outputFile << "-------------------------------\n" << std::endl;

    // std::cout << "SumIO \t" << timeOpenClose.count() << std::endl;
}

/**
 * Read all passed files with the passed engine.
 * As the BP files containing the transformed NetCDF data only have one step
 * currently, there is only one step time interval. So calculating mean time is
 * not sensible.
 *
 * This behaviour of writing the BP files is intentional.
 * There are two solutions that both are not ideal!
 *
 * 1) The NetCDF variables are read in one at a time. To be able to directly
 * write this data to an ADIOS2 variable steps cannot be used. Otherwise the
 * variables would have continuous step numbers, because endStep increases the
 * step counter. So that, e.g. the second variable cannot start at 0 as this
 * step is already used by the first variable.
 *
 * However, the NetCDF variables belong together so separating logically
 * concurrent data into different steps is not useful. These can start capture
 * the relationships, so that every first time entry is mapped to blockID one.
 *
 * 2) Another possibility would be first read the complete NetCDF data and to
 * store it for every NetCDF variable in a large buffer, that is written after
 * reading the complete NetCDF file. As the file sizes easily reach a
 * significant order such as 30-100 GB temporarily storing all this data is not
 * feasible. This is why I decided to map the steps of the NetCDF data to ADIOS2
 * blocks. Since NetCDF has no concept of blocks this is not a problem.
 *
 * Maybe using SetStepSelection will help to use steps correctly.
 * SetBlockSelection works only for reading ADIOS2 variables. So, I am not sure.
 * Unfortunately, there was no time for detailed testing until now.
 *
 * @param engineName      engine Reader
 * @param path            directory or file to read
 * @param filesToRead     number of files to read
 * @param variablesToRead number of variables to read
 */
void AdiosRead(std::string engineName, std::string path, size_t filesToRead,
               uint32_t variablesToRead)
{

    time_point<Clock> startOpen;     // start time of complete I/O
    time_point<Clock> startStep;     // start time of step
    time_point<Clock> startGets;     // start time of reading all blocks
    time_point<Clock> startGetBlock; // start time of reading block

    time_point<Clock> endGetBlock; // end time of reading block
    time_point<Clock> endGets;     // end time of reading all blocks
    time_point<Clock> endStep;     // end time of step
    time_point<Clock> endOpen;     // end time of complete I/O

    milliseconds blockDelta; // time interval to read one block
    nanoseconds blockDeltaNANO; // time interval to read one block
    milliseconds getDelta;   // time interval to read all blocks

    std::vector<nanoseconds>
        getBlockDeltaNANO; // all times intervals to read a block
    std::vector<milliseconds>
        getBlockDelta; // all times intervals to read a block
    std::vector<milliseconds>
        getsDelta; // all times intervals to read all blocks

    std::time_t curr_time;
    std::ofstream outputFile;
    std::string debugFileName;
    buildDebugFileName(debugFileName, engineName, path, filesToRead,
                       variablesToRead, curr_time);
    outputFile.open(debugFileName);
    printDebugHeader(outputFile, curr_time);

    size_t fileCount = 0; // loop counter
    std::string varName;
    std::vector<std::string> files;

    readInput(path, files, outputFile);

    adios2::ADIOS adios(adios2::DebugON);

    for (auto &file : files)
    {
        if (filesToRead == fileCount)
        {
            outputFile << "filesToRead: " << filesToRead
                       << " fileCount: " << fileCount << std::endl;
            continue;
        }
        std::string ioName = "Output-" + std::to_string(fileCount);
        outputFile << "\n-------------------------------" << std::endl;
        outputFile << "ioName: " << ioName << std::endl;

        adios2::IO io = adios.DeclareIO(ioName);
        io.SetEngine(engineName);
        std::cout << "FileName: " << file << std::endl;
        outputFile << "FileName: " << file << std::endl;

        size_t steps = 0;
        size_t varCount = 0; // loop couner

        startOpen = Clock::now(); // start time complete I/O

        adios2::Engine reader = io.Open(file, adios2::Mode::Read);
        auto varMap = io.AvailableVariables();

        reader.BeginStep(adios2::StepMode::Read);
        startStep = Clock::now(); // start stime of step

        for (const auto &var : varMap)
        {
            if (variablesToRead == varCount)
            {
                outputFile << "varCount: " << varCount
                           << " variablesToRead: " << variablesToRead
                           << std::endl;
                continue;
            }

            // TODO: maybe use SetStepSelection before Step loop
            varName = var.first;
            adios2::Params params = var.second;
            std::cout << "\n " << varName << std::endl;
            outputFile << "\n " << varName << std::endl;

            if (strcmp(varName.c_str(), "time") == 0)
            {
                continue;
            }

            auto type = io.VariableType(varName);

            std::vector<float> test(128);
            if (type == "compound")
            {
            }
#define declare_type(T)                                                        \
    else if (type == adios2::GetType<T>())                                     \
    {                                                                          \
        auto variable = io.InquireVariable<T>(varName);                        \
                                                                               \
        steps = variable.Steps();                                              \
                                                                               \
        for (size_t step = 0; step < steps; step++)                            \
        {                                                                      \
            auto blocksInfo = reader.BlocksInfo(variable, step);               \
                                                                               \
            if (false)                                                         \
            {                                                                  \
                std::cout << "type: " << type << std::endl;                    \
                std::cout << "shape size: " << variable.Shape().size()         \
                          << std::endl;                                        \
                std::cout << "steps: " << steps << std::endl;                  \
                std::cout << "number of blocks = " << blocksInfo.size()        \
                          << std::endl;                                        \
            }                                                                  \
                                                                               \
            std::vector<std::vector<T>> dataSet;                               \
            dataSet.resize(blocksInfo.size());                                 \
            outputFile << "BlkCnt \t" << blocksInfo.size() << std::endl;       \
                                                                               \
            size_t i = 0;                                                      \
            startGets = Clock::now();                                          \
                                                                               \
            for (auto &info : blocksInfo)                                      \
            {                                                                  \
                                                                               \
                variable.SetBlockSelection(info.BlockID);                      \
                startGetBlock = Clock::now();                                  \
                reader.Get<T>(variable, dataSet[i], adios2::Mode::Sync);       \
                                                                               \
                endGetBlock = Clock::now();                                    \
                blockDelta =                                                   \
                    duration_cast<milliseconds>(endGetBlock - startGetBlock);  \
                blockDeltaNANO = \
                    duration_cast<nanoseconds>(endGetBlock - startGetBlock);  \
                getBlockDelta.push_back(blockDelta);                           \
                getBlockDeltaNANO.push_back(blockDeltaNANO);                           \
                ++i;                                                           \
            }                                                                  \
            if (blocksInfo.size() > 0)                                                         \
            {                                                                  \
                endGets = Clock::now();                                        \
                getDelta = duration_cast<milliseconds>(endGets - startGets);   \
                getsDelta.push_back(getDelta);                                 \
            }                                                                  \
        }                                                                      \
    }
            ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
            varCount++;
            // caculateMeanBlockTime(outputFile);
            // caculateMeanGetsTime(outputFile);
            calculateMeanTime(outputFile, getBlockDelta, false);
            calculateMeanTime(outputFile, getsDelta, true);
            std::cout << "Nano: "<< getBlockDeltaNANO[0].count() << std::endl;
            getsDelta.clear();
            getBlockDelta.clear();
            getBlockDeltaNANO.clear();

        } // end for varMap loop

        reader.PerformGets();
        reader.EndStep();
        endStep = Clock::now();

        reader.Close();
        endOpen = Clock::now();

            milliseconds timeOpenClose =
        duration_cast<milliseconds>(endOpen - startOpen);
    milliseconds timeStep = duration_cast<milliseconds>(endStep - startStep);

    outputFile << "\nStep \t" << timeStep.count() << std::endl;
    outputFile << "SumIO \t" << timeOpenClose.count() << std::endl;
    outputFile << "-------------------------------\n" << std::endl;

    std::cout << "\nStep \t" << timeStep.count() << std::endl;
    std::cout << "SumIO \t" << timeOpenClose.count() << std::endl;
        // calculateIOTime(outputFile);
        fileCount++;
    } // end for files loop
}

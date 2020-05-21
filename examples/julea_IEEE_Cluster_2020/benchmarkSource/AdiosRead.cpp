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
#include <iomanip>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

time_point<Clock> startOpen;
time_point<Clock> startStep;
time_point<Clock> startGets;
time_point<Clock> startGetBlock;

std::vector<time_point<Clock>> getBlockTimes;
milliseconds blockDelta;
milliseconds getDelta;
std::vector<milliseconds> getBlockDelta;
std::vector<milliseconds> getsDelta;

time_point<Clock> endGetBlock;
time_point<Clock> endGets;
time_point<Clock> endStep;
time_point<Clock> endOpen;
// using namespace std::literals::chrono_literals;
// using std::this_thread::sleep_for;

// #include "AdiosQuery.h"

void readDirectory(const std::string &path, std::vector<std::string> &v)
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
            std::cout << "is .dir" << std::endl;
            continue;
        }
        std::cout << "Slen: " << slen << std::endl;

        completeFileName = path + "/" + dirEntry->d_name;
        v.push_back(completeFileName);
        std::cout << dirEntry->d_name << std::endl;
        std::cout << completeFileName << "\n" << std::endl;
    }
    closedir(dirPtr);
}

void readInput(const std::string &path, std::vector<std::string> &files)
{

    struct stat s;
    if (stat(path.c_str(), &s) == 0)
    {
        if (s.st_mode & S_IFDIR)
        {
            // it's a directory
            std::cout << "directory contains: " << std::endl;
            readDirectory(path, files);
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
}

void AdiosReadMinMax(std::string path, std::string variableName)
{
    std::cout << "AdiosReadMinMax" << std::endl;
}

void caculateMeanBlockTime()
{
    size_t sumTimes = 0;
    size_t mean = 0;
    for (auto &times : getBlockDelta)
    {
        sumTimes += times.count();
        // std::cout << "getBlockDelta: " << times.count() << std::endl;
    }
    mean = (sumTimes / getBlockDelta.size()) ;
    // std::cout << "getBlockDelta.size(): " << getBlockDelta.size() << std::endl;
    // std::cout << "sumTimes: " << sumTimes << std::endl;
     // std::cout << "Average time to read a block: " << mean << " ms"
    // << std::endl;
    std::cout << "Block \t" << mean << std::endl;
}

void caculateMeanGetsTime()
{
    size_t sumTimes = 0 ;
    size_t mean = 0;
    for (auto &times : getsDelta)
    {
        sumTimes += times.count();
        // std::cout << "getsDelta: " << times.count() << std::endl;
    }
    mean = (sumTimes / getsDelta.size());
    // std::cout << "sumTimes: " << sumTimes << std::endl;
    // std::cout << "Average time to get all blocks: " << mean << " ms"
    // << std::endl;
    // std::cout << "getsDelta.size(): " << getsDelta.size() << std::endl;
    std::cout << "AllBl \t" << mean << std::endl;
}

void calculateStatistics()
{
    milliseconds timeOpenClose =
        duration_cast<milliseconds>(endOpen - startOpen);
    milliseconds timeStep = duration_cast<milliseconds>(endStep - startStep);
    milliseconds timeGets = duration_cast<milliseconds>(endGets - startGets);
    milliseconds timeGetBlocks =
        duration_cast<milliseconds>(endGetBlock - startGetBlock);

    // std::cout << "Time from open to close: " << timeOpenClose.count() << "
    // ms"
    //           << std::endl;
    std::cout << "Step \t" << timeStep.count() << std::endl;
    std::cout << "SumIO \t" << timeOpenClose.count() << std::endl;
    // std::cout << "complete read time: " << timeGets.count() << " ms"
    //           << std::endl;
    // std::cout << "read block time: " << timeGetBlocks.count() << " ms"
    //           << std::endl;
}

void AdiosRead(std::string engineName, std::string path, size_t filesToRead,
               uint32_t variablesToRead)
{
    // std::cout << "AdiosRead" << std::endl;
    size_t fileCount = 0; // loop counter
    std::string varName;
    std::vector<std::string> files;

    readInput(path, files);

    adios2::ADIOS adios(adios2::DebugON);

    for (auto &file : files)
    {
        if (filesToRead == fileCount)
        {
            std::cout << "filesToRead: " << filesToRead
                      << " fileCount: " << fileCount << std::endl;
            continue;
        }
        std::string ioName = "Output-" + std::to_string(fileCount);
        std::cout << "ioName: " << ioName << std::endl;

        // adios2::IO io = adios.DeclareIO("Output");
        adios2::IO io = adios.DeclareIO(ioName);
        io.SetEngine(engineName);
        std::cout << "FileName: " << file << std::endl;

        size_t steps = 0;
        size_t varCount = 0; // loop couner

        startOpen = Clock::now();
        // adios2::Engine reader = io.Open(files[2], adios2::Mode::Read);
        adios2::Engine reader = io.Open(file, adios2::Mode::Read);
        auto varMap = io.AvailableVariables();

        reader.BeginStep(adios2::StepMode::Read);
        startStep = Clock::now();

        for (const auto &var : varMap)
        {
            if (variablesToRead == varCount)
            {
                std::cout << "varCount: " << varCount
                          << " variablesToRead: " << variablesToRead
                          << std::endl;
                continue;
            }

            // TODO: maybe use SetStepSelection before Step loop
            varName = var.first;
            adios2::Params params = var.second;
            // std::cout << "\nvarName: " << varName << std::endl;

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
                getBlockDelta.push_back(blockDelta);                           \
                ++i;                                                           \
            }                                                                  \
            if (i > 1)                                                         \
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
            // std::cout << "-------------------------" << std::endl;
        } // end for varMap loop
        reader.PerformGets();
        reader.EndStep();
        endStep = Clock::now();
        reader.Close();
        endOpen = Clock::now();

        caculateMeanBlockTime();
        caculateMeanGetsTime();
        calculateStatistics();
        fileCount++;
    } // end for files loop
}
// std::cout << "front: " << dataSet[i].front() << std::endl;     \
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
#include <adios2.h>
#include <chrono>
#include <dirent.h>
#include <iomanip>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;

time_point<Clock> startOpen;
time_point<Clock> startStep;
time_point<Clock> startGets;
time_point<Clock> startGetBlock;

time_point<Clock> endGetBlock;
time_point<Clock> endGets;
time_point<Clock> endStep;
time_point<Clock> endOpen;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
// using namespace std::literals::chrono_literals;
// using std::this_thread::sleep_for;

// #include "AdiosQuery.h"

void readDirectory(const std::string &path, std::vector<std::string> &v)
{
    DIR *dirPtr = opendir(path.c_str());
    struct dirent *dirEntry;
    while ((dirEntry = readdir(dirPtr)) != NULL)
    {
        v.push_back(dirEntry->d_name);
        std::cout << dirEntry->d_name << std::endl;
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

void AdiosReadMinMax(std::string fileName, std::string variableName)
{
    std::cout << "AdiosReadMinMax" << std::endl;
}

void calculateStatistics()
{
    // time_point<Clock> timeOpenClose = endOpen - startOpen;
    milliseconds timeOpenClose = duration_cast<milliseconds>(endOpen - startOpen);
    milliseconds timeStep = duration_cast<milliseconds>(endStep - startStep);
    milliseconds timeGets = duration_cast<milliseconds>(endGets - startGets);
    milliseconds timeGetBlocks = duration_cast<milliseconds>(endGetBlock - startGetBlock);

    std::cout << "Time from open to close: " << timeOpenClose.count() << " ms"<< std::endl;
    std::cout << "step duration: " << timeStep.count() << " ms"<< std::endl;
    std::cout << "complete read time: " << timeGets.count() << " ms"<< std::endl;
    std::cout << "read block time: " << timeGetBlocks.count() << " ms"<< std::endl;
}

void AdiosRead(std::string engineName, std::string path, size_t fileCount,
               uint32_t percentageVarsToRead)
{
    // time_point<Clock> startOpen;
    // time_point<Clock> startStep;
    // time_point<Clock> startGets;
    // time_point<Clock> startGetBlock;

    // time_point<Clock> endGetBlock;
    // time_point<Clock> endGets;
    // time_point<Clock> endStep;
    // time_point<Clock> endOpen;

    std::cout << "AdiosRead" << std::endl;
    std::vector<std::string> files;

    readInput(path, files);
    std::string varName;

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engineName);

    for (auto &file : files)
    {

        // read first fileCount
        // read variables but only percentage
        size_t steps = 0;
        size_t varCount = 0;

        startOpen = Clock::now();
        adios2::Engine reader = io.Open(file, adios2::Mode::Read);
        auto varMap = io.AvailableVariables();

        reader.BeginStep(adios2::StepMode::Read);
        startStep = Clock::now();

        for (const auto &var : varMap)
        {
            // TODO: maybe use SetStepSelection before Step loop
            varName = var.first;
            adios2::Params params = var.second;
            std::cout << "\nvarName: " << varName << std::endl;

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
            if (true)                                                         \
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
                ++i;                                                           \
            }                                                                  \
            endGets = Clock::now();                                            \
        }                                                                      \
    }
            ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
            varCount++;
            std::cout << "-------------------------" << std::endl;
        }
        reader.PerformGets();
        reader.EndStep();
        endStep = Clock::now();
        reader.Close();
        endOpen = Clock::now();
        calculateStatistics();
    }
}
// std::cout << "front: " << dataSet[i].front() << std::endl;     \
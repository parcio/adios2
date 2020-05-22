/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application demonstrating some of the query possibilities enabled by the
 * JULEA database engine.
 *
 *  Created on: May 22, 2020
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
#include <numeric>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

void buildMeanDebugFileName(std::string &fileName, std::string engineName,
                            std::string path, std::time_t &curr_time)
{
    std::ofstream outputFile;
    auto currentTime = std::chrono::system_clock::now();
    curr_time = std::chrono::system_clock::to_time_t(currentTime);

    char timeBuffer[80];
    std::tm *timeinfo;
    timeinfo = localtime(&curr_time);
    strftime(timeBuffer, 80, "-%Y-%m-%d-%I:%M%p", timeinfo);

    fileName = engineName + "-" + path + "-" + timeBuffer + ".txt";
}

void printMeanDebugHeader(std::ofstream &outputFile, std::time_t curr_time)
{
    outputFile << "--- AdiosMean ---" << std::endl;
    outputFile << "Current time: " << std::ctime(&curr_time);
    outputFile << "\nvariableName \n"
               << "AllBl:  \tTime to read all blocks that have a min "
                  "smaller/max larger than compareValue in ms \n\n"
               << "Larger: \tNumber of blocks that have a min smaller/max "
                  "larger than compareValue"
               << "Step:   \tTime for a step in ms\n"
               << "SumIO:  \tTime for complete I/O in ms\n"
               << std::endl;
    outputFile << "-------------------------------" << std::endl;
}

void AdiosMean(std::string engineName, std::string path, size_t filesToRead,
               std::string variableToCalculateMeanOn)
{
    // std::cout << "--- AdiosReadMinMax--- engine: " << engineName <<
    // std::endl;
    time_point<Clock> startOpen;      // start time of complete I/O
    time_point<Clock> startStep;      // start time of step
    time_point<Clock> startGetBlocks; // start time of reading all blocks
    time_point<Clock> startGetBlock;  // start time of reading block

    time_point<Clock> endGetBlock;  // end time of reading block
    time_point<Clock> endGetBlocks; // end time of reading all blocks
    time_point<Clock> endStep;      // end time of step
    time_point<Clock> endOpen;      // end time of complete I/O

    milliseconds blockDelta;     // time interval to read one block
    milliseconds getBlocksDelta; // time interval to read all blocks

    std::vector<milliseconds>
        getBlocksDeltaVector; // time intervals to read all blocks
    std::vector<milliseconds>
        blockDeltaVector; // time intervals to read one block

    std::time_t curr_time;
    std::ofstream outputFile;
    std::string debugFileName;

    buildMeanDebugFileName(debugFileName, engineName, path, curr_time);
    outputFile.open(debugFileName);
    printMeanDebugHeader(outputFile, curr_time);

    // std::cout << "debugFileName: " << debugFileName << std::endl;

    size_t fileCount = 0; // loop counter
    size_t larger = 0;    // number of blocks with larger Max than compareValue
    double sum = 0.0;
    double mean = 0.0;

    std::string varName;
    std::vector<std::string> files;

    readInput(path, files, outputFile);
    // files.push_back("Test.bp");
    // std::cout << "DEBUG 1" << std::endl;

    adios2::ADIOS adios(adios2::DebugON);

    for (auto &file : files)
    {
        // std::cout << "DEBUG 2" << std::endl;
        if (filesToRead == fileCount)
        {
            outputFile << "filesToRead: " << filesToRead
                       << " fileCount: " << fileCount << std::endl;
            continue;
        }
        std::string ioName = "Output-" + std::to_string(fileCount);
        outputFile << "\n-------------------------------" << std::endl;
        outputFile << "ioName: " << ioName << std::endl;
        // std::cout << "ioName: " << ioName << std::endl;

        adios2::IO io = adios.DeclareIO(ioName);
        io.SetEngine(engineName);
        // std::cout << "FileName: " << file << std::endl;
        outputFile << "FileName: " << file << std::endl;

        size_t steps = 0;
        size_t varCount = 0; // loop counter

        startOpen = Clock::now(); // start time complete I/O

        adios2::Engine reader = io.Open(file, adios2::Mode::Read);
        auto varMap = io.AvailableVariables();

        reader.BeginStep(adios2::StepMode::Read);
        startStep = Clock::now(); // start time of step

        // for (const auto &var : varMap)
        // {
        //     if (variablesToRead == varCount)
        //     {
        //         outputFile << "varCount: " << varCount
        //                    << " variablesToRead: " << variablesToRead
        //                    << std::endl;
        //         continue;
        //     }

        // TODO: maybe use SetStepSelection before Step loop
        // varName = var.first;
        // adios2::Params params = var.second;
        // std::cout << "\n " << varName << std::endl;
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
            size_t i = 0;                                                      \
            startGetBlocks = Clock::now();                                     \
            auto blocksInfo = reader.BlocksInfo(variable, step);               \
                                                                               \
            std::vector<std::vector<T>> dataSet;                               \
            dataSet.resize(blocksInfo.size());                                 \
            outputFile << "BlkCnt \t" << blocksInfo.size() << std::endl;       \
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
                blockDeltaVector.push_back(blockDelta);                        \
                ++i;                                                           \
            }                                                                  \
            endGetBlocks = Clock::now();                                       \
            getBlocksDelta =                                                   \
                duration_cast<milliseconds>(endGetBlocks - startGetBlocks);    \
            getBlocksDeltaVector.push_back(getBlocksDelta);                    \
        }                                                                      \
    }
        ADIOS2_FOREACH_ATTRIBUTE_PRIMITIVE_STDTYPE_1ARG(declare_type)
#undef declare_type
        varCount++;

        calculateMeanTime(outputFile, getBlocksDeltaVector, true);
        // calculateMeanTime(outputFile, getsDelta, true);

        // sum = std::accumulate(dataSet.begin(), dataSet.end(), 0.0);\
        //         mean = sum / v.size();\

        // getsDelta.clear();
        // getBlockDelta.clear();
        // } // end for varMap loop

        reader.PerformGets();
        reader.EndStep();
        endStep = Clock::now();

        reader.Close();
        endOpen = Clock::now();

        milliseconds timeOpenClose =
            duration_cast<milliseconds>(endOpen - startOpen);
        milliseconds timeStep =
            duration_cast<milliseconds>(endStep - startStep);

        outputFile << "Larger \t" << larger << std::endl;
        outputFile << "\nStep \t" << timeStep.count() << std::endl;
        outputFile << "SumIO \t" << timeOpenClose.count() << std::endl;
        outputFile << "-------------------------------\n" << std::endl;

        // std::cout << "\nStep \t" << timeStep.count() << std::endl;
        std::cout << "Larger \t" << larger << std::endl;
        std::cout << "SumIO \t" << timeOpenClose.count() << std::endl;

        fileCount++;
    } // end for files loop
}
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
#include <iomanip>
#include <iostream>
#include <vector>

// #include "AdiosQuery.h"

void AdiosReadMinMax(std::string fileName, std::string variableName)
{
    std::cout << "AdiosReadMinMax" << std::endl;
}

void AdiosRead(std::string engineName, std::string directory, size_t fileCount,
               uint32_t percentageVarsToRead)
{
    std::cout << "AdiosRead" << std::endl;

    // is directory? is file?
    // get all files
    // read first fileCount
    // read variables but only percentage
    // std::string fileName;
    std::string fileName = "sresa1b_ncar_ccsm3-example.bp";
    size_t steps = 0;
    size_t stepsStart = 0;
    size_t varCount = 0;
    std::string varName;

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engineName);

    adios2::Engine reader = io.Open(fileName, adios2::Mode::Read);
    auto varMap = io.AvailableVariables();
    for (const auto &var : varMap)
    {
        varName = var.first;
        adios2::Params params = var.second;
        std::cout << "varName: " << varName << std::endl;
        auto type = io.VariableType(varName);

        std::vector<float> test(128);
        if (type == "compound")
        {
        }
#define declare_type(T)                                                        \
    else if (type == adios2::GetType<T>())                                     \
    {                                                                          \
        auto variable = io.InquireVariable<T>(varName);                        \
        auto variable2 = io.InquireVariable<float>(varName);                   \
        adios2::Dims shape = variable.Shape();                                 \
        adios2::Dims start = variable.Start();                                 \
        adios2::Dims count = variable.Count();                                 \
        std::cout << "shape size: " << shape.size() << std::endl;              \
        std::cout << "shape front: " << shape[0] << std::endl;                 \
        std::cout << "start size: " << start.size() << std::endl;              \
        std::cout << "start front: " << start.front() << std::endl;            \
        std::cout << "count size: " << count.size() << std::endl;              \
        std::cout << "count front: " << count.front() << std::endl;            \
        std::cout << "type: " << type << std::endl;                            \
        steps = variable.Steps();                                              \
        std::cout << "steps: " << steps << std::endl;                          \
        for (size_t step = 0; step < steps; step++)                            \
        {                                                                      \
            reader.BeginStep(adios2::StepMode::Read);                          \
            std::cout << "step: " << step << std::endl;                        \
            stepsStart = variable.StepsStart();                                \
            auto blocksInfo = reader.BlocksInfo(variable, step);               \
            std::cout << "number of blocks = " << blocksInfo.size()            \
                      << std::endl;                                            \
            std::vector<std::vector<T>> dataSet;                               \
            dataSet.resize(blocksInfo.size());                                 \
            std::cout << "sizeof(dataSet): " << sizeof(dataSet) << std::endl;  \
            size_t i = 0;                                                      \
            for (auto &info : blocksInfo)                                      \
            {                                                                  \
                std::cout << "i: " << i << std::endl;                          \
                std::cout << "block loop " << std::endl;                       \
                std::cout << "blockID: " << info.BlockID << std::endl;         \
                variable.SetBlockSelection(info.BlockID);                      \
                std::cout << "reached" << std::endl;                           \
                if (varCount == 1)                                             \
                {                                                              \
                    reader.Get<float>(variable2, test.data(),                  \
                                      adios2::Mode::Deferred);                 \
                }                                                              \
                else                                                           \
                {                                                              \
                    reader.Get<T>(variable, dataSet[i],                        \
                                  adios2::Mode::Deferred);                     \
                }                                                              \
                std::cout << "size: " << dataSet.size() << std::endl;          \
                std::cout << "size: " << dataSet[i].size() << std::endl;       \
                std::cout << "front: " << dataSet[i].front() << std::endl;     \
                std::cout << "reached2" << std::endl;                          \
                ++i;                                                           \
            }                                                                  \
            reader.PerformGets();                                              \
            reader.EndStep();                                                  \
        }                                                                      \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
        varCount++;
    }
}
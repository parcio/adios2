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
    std::string fileName2 = "_grib2netcdf-webmars-public-svc-blue-004-"
                            "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.bp";
    size_t steps = 0;
    size_t stepsStart = 0;
    size_t varCount = 0;
    std::string varName;

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engineName);

    adios2::Engine reader = io.Open(fileName, adios2::Mode::Read);
    auto varMap = io.AvailableVariables();
    reader.BeginStep(adios2::StepMode::Read);
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
            stepsStart = variable.StepsStart();                                \
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
            size_t i = 0;                                                      \
            for (auto &info : blocksInfo)                                      \
            {                                                                  \
                variable.SetBlockSelection(info.BlockID);                      \
                reader.Get<T>(variable, dataSet[i], adios2::Mode::Sync);       \
                ++i;                                                           \
            }                                                                  \
        }                                                                      \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
        varCount++;
        std::cout << "-------------------------" << std::endl;
    }
    reader.PerformGets();
    reader.EndStep();
}
// std::cout << "front: " << dataSet[i].front() << std::endl;     \
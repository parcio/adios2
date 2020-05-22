/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application to convert a NetCDF4 file of unknown dimensions into a ADIOS2
 * file. All variables are read and then translated into their according ADIOS2
 * equivalent. Note: entries along the time dimensions are treated as individual
 * blocks.
 *
 * Created on: May 3, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include "NetCDFRead.h"
#include <adios2.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <netcdf>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

void NCbuildDebugFileName(std::string &fileName, std::string engineName,
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

void NCprintDebugHeader(std::ofstream &outputFile, std::time_t curr_time,
                        std::string ncFileName, std::string adiosFileName)
{
    outputFile << "--- NCRead ---" << std::endl;
    outputFile << "NetCDF4 file: " << ncFileName << std::endl;
    outputFile << "ADIOS2 file: " << adiosFileName << "\n" << std::endl;
    // std::cout << "engine: " << engine << std::endl;

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

size_t getTime(std::ofstream &outputFile, std::vector<milliseconds> &delta)
{
    size_t sumTimes = 0;
    // size_t mean = 0;
    for (auto &times : delta)
    {
        sumTimes += times.count();
        // std::cout << "delta: " << times.count() << std::endl;
    }
    // mean = (sumTimes / delta.size());
    return sumTimes;
}

void NCcalculateMeanTime(std::ofstream &outputFile,
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

std::string mapNCTypeToAdiosType(size_t typeID)
{
    const char *type;
    if (typeID == NC_BYTE)
    {
        type = "int8_t";
    }
    else if (typeID == NC_CHAR)
    {
        type = "int8_t";
    }
    else if (typeID == NC_SHORT)
    {
        type = "int16_t";
    }
    else if (typeID == NC_INT)
    {
        type = "int32_t";
    }
    else if (typeID == NC_FLOAT)
    {
        type = "float";
    }
    else if (typeID == NC_DOUBLE)
    {
        type = "double";
    }
    else if (typeID == NC_UBYTE)
    {
    }
    else if (typeID == NC_USHORT)
    {
        type = "uint16_t";
    }
    else if (typeID == NC_UINT)
    {
        type = "uint32_t";
    }
    else if (typeID == NC_INT64)
    {
        type = "int64_t";
    }
    else if (typeID == NC_UINT64)
    {
        type = "uint64_t";
    }
    else if (typeID == NC_STRING)
    {
        //"std::string (only used for global and local values, not arrays)"
        // adios2 docu
        // https://adios2.readthedocs.io/en/latest/components/components.html#variable
    }
    else if (typeID == NC_VLEN)
    {
        std::cout << "typeID: " << NC_VLEN << " currently not supported!"
                  << std::endl;
    }
    else if (typeID == NC_OPAQUE)
    {
        std::cout << "typeID: " << NC_OPAQUE << " currently not supported!"
                  << std::endl;
    }
    else if (typeID == NC_ENUM)
    {
        std::cout << "typeID: " << NC_ENUM << " currently not supported!"
                  << std::endl;
    }
    else if (typeID == NC_COMPOUND)
    {
        std::cout << "typeID: " << NC_COMPOUND << " currently not supported!"
                  << std::endl;
    }
    return std::string(type);
}

template <class T>
void transformValues(std::string varName, netCDF::NcVar variable, T *data,
                     size_t dataSize, float *data2)
{
    std::cout << "--- transformValues T " << std::endl;
}

template <>
void transformValues<int16_t>(std::string varName, netCDF::NcVar variable,
                              int16_t *data, size_t dataSize, float *data2)
{
    // std::cout << "--- transformValues " << std::endl;

    auto varAttrMap = variable.getAtts();
    auto parentGroup = variable.getParentGroup();
    auto groupAttrMap = parentGroup.getAtts();

    std::string scaleName = "scale_factor";
    auto scaleFactor = variable.getAtt(scaleName);

    std::string offsetName = "add_offset";
    auto offSet = variable.getAtt(offsetName);

    double scale;
    double offset;

    scaleFactor.getValues(&scale);
    offSet.getValues(&offset);

    // std::cout << "scale: " << scale << std::endl;
    // std::cout << "offset: " << offset << std::endl;

    auto numberElements = dataSize / sizeof(short);
    // std::cout << "numberElements: " << numberElements << std::endl;
    auto currentValue = 0;

    for (int i = 0; i < numberElements; i++)
    {
        currentValue += currentValue;

        std::string adiosType = "int16_t";

        data2[i] = (float)(data[i] * scale + offset);
        // std::cout << "data1: " << data[i] << std::endl;
        // std::cout << "data2: " << data2[i] << std::endl;
    }
}

/**
 * Read the passed NetCDF file and transform all variables to ADIOS2 variables.
 *
 * Important note:
 * The BP files will only have one step! The step concept of NetCDF is mapped to
 * blocks.
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
 * concurrent data into different steps is not useful. The record concept
 * (having multiple entries in the time variable) is mapped to ADIOS2 blocks.
 * These can start capture the relationships, so that every first time entry is
 * mapped to blockID one.
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
 * [NCReadFile description]
 * @param engine          [description]
 * @param ncFileName      [description]
 * @param adiosFileName   [description]
 * @param printDimensions [description]
 * @param printVariable   [description]
 */
void NCReadFile(std::string engine, std::string ncFileName,
                std::string adiosFileName, bool printDimensions,
                bool printVariable, bool needsTransform)
{
    time_point<Clock> startOpen;     // start time of complete I/O
    time_point<Clock> startStep;     // start time of step
    time_point<Clock> startPuts;     // start time of reading all blocks
    time_point<Clock> startPutBlock; // start time of reading block

    time_point<Clock> endPutBlock; // end time of writing block
    time_point<Clock> endPuts;     // end time of writing all blocks
    time_point<Clock> endStep;     // end time of step
    time_point<Clock> endOpen;     // end time of complete I/O

    milliseconds blockDelta; // time interval to write one block
    milliseconds putDelta;   // time interval to write all blocks

    std::vector<milliseconds> putBlockDelta; // time intervals to write a block
    std::vector<milliseconds> putsDelta; // time intervals to write all blocks

    time_point<Clock> startNcStuff;
    time_point<Clock> startNcGet;
    time_point<Clock> endNcStuff;
    time_point<Clock> endNcGet;

    milliseconds ncStuffDelta;
    milliseconds ncGetDelta;

    std::vector<milliseconds> ncStuffDeltaVector;
    std::vector<milliseconds> ncGetDeltaVector;
    std::vector<size_t> sumNcGetDeltaVector;

    std::time_t curr_time;
    std::ofstream outputFile;
    std::string debugFileName;

    NCbuildDebugFileName(debugFileName, engine, adiosFileName, curr_time);
    outputFile.open(debugFileName);
    NCprintDebugHeader(outputFile, curr_time, ncFileName, adiosFileName);

    bool hasSteps = false;
    // bool needsTransform = false;
    bool isTime = false;
    size_t varCount = 0;
    size_t dimCount = 0;
    size_t dimsID = 0;
    size_t dimsSize = 0;
    size_t numberSteps = 0;
    size_t dataSize = 1;

    /** ADIOS2 open file ... etc. */
    adios2::ADIOS adios(adios2::DebugON);

    /** netCDF4 open file ... etc. */
    netCDF::NcFile dataFile;
    dataFile.open(ncFileName, netCDF::NcFile::read);

    auto fileDims = dataFile.getDims();

    if (printDimensions)
    {

        std::cout << "number of file dimensions: " << fileDims.size()
                  << std::endl;
    }

    /** all dimensions declared for the nc file */
    for (const auto &dim : fileDims)
    {
        std::string name = dim.first;
        netCDF::NcDim dimension = dim.second;

        if (printDimensions)
        {
            std::cout << "dimension name: " << name << std::endl;
        }
    }

    // auto groupAttrMap = dataFile.getAtts();
    // std::cout << "number of attributes: " << groupAttrMap.size() <<
    // std::endl; for (const auto &attr : groupAttrMap)
    // {
    //     std::string attrName = attr.first;
    //     netCDF::NcGroupAtt attribute = attr.second;
    //     std::cout << "group attribute name:" << attrName << std::endl;
    // }

    auto varMap = dataFile.getVars();

    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);
    // std::cout << "adiosFileName: " << adiosFileName << std::endl;

    startOpen = Clock::now(); // start time complete I/O

    adios2::Engine writer = io.Open(adiosFileName, adios2::Mode::Write);
    startNcStuff = Clock::now();

    /** all variables declared in the nc file */
    for (const auto &var : varMap)
    {

        std::string name = var.first;
        netCDF::NcVar variable = var.second;
        // std::cout << "\n " << name << std::endl;
        outputFile << "\n " << name << std::endl;

        netCDF::NcType type = variable.getType();
        auto typeID = type.getId();
        auto typeName = type.getName();
        std::vector<netCDF::NcDim> varDims = variable.getDims();

        auto varAttrMap = variable.getAtts();

        adios2::Dims shape;
        adios2::Dims start;
        adios2::Dims count;

        std::vector<size_t> ncStart;
        std::vector<size_t> ncCount;

        dimCount = 0;
        dataSize = 1;
        hasSteps = 0;
        isTime = 0;

        if (printVariable)
        {
            std::cout << "\n---------------------- variable: " << varCount
                      << "----------------------" << std::endl;
            std::cout << "" << name << " - " << type.getName() << ":"
                      << std::endl;
        }

        /** all dimensions for the current variable */
        for (const auto &dims : varDims)
        {

            std::string dimsName = dims.getName();
            dimsSize = dims.getSize();

            /** if variable is time variable */
            if ((strcmp(dimsName.c_str(), "time") == 0) &&
                (varDims.size() == 1))
            {
                hasSteps = true;
                isTime = true;
                numberSteps = dimsSize;
                if (dimsSize == 0)
                {
                    numberSteps = 1;
                }
                // ncStart.push_back(0);
                // ncCount.push_back(1);
                shape.push_back(dimsSize);
                start.push_back(0);
                count.push_back(dimsSize);

                ncStart.push_back(0);
                ncCount.push_back(1);
            }
            /** if variable has time dependency */
            else if (strcmp(dimsName.c_str(), "time") == 0)
            {
                hasSteps = true;
                numberSteps = dimsSize;
                ncStart.push_back(0);
                ncCount.push_back(1);
            }
            else
            {
                dataSize = dataSize * dimsSize;

                shape.push_back(dimsSize);
                start.push_back(0);
                count.push_back(dimsSize);

                if (hasSteps)
                {
                    ncStart.push_back(0);
                    ncCount.push_back(dimsSize);
                }
            }

            if (printVariable)
            {
                std::cout << "\n-- Dim: " << dimCount + 1 << std::endl;
                std::cout << "Name: " << dims.getName() << std::endl;
                std::cout << "getID: " << dims.getId() << std::endl;
                std::cout << "size: " << dims.getSize() << std::endl;
                std::cout << "isUnlimited: " << dims.isUnlimited() << std::endl;

                std::cout << "hasSteps: " << hasSteps << std::endl;
                std::cout << "numberSteps: " << numberSteps << std::endl;
            }
            ++dimCount;
        }

        std::string adiosType = mapNCTypeToAdiosType(typeID);

        /** Define and write ADIOS 2 variable */
        if (adiosType == "compound")
        {
        }
        else if ((adiosType == "int16_t") && needsTransform)
        {
            adios2::Variable<float> adiosVar;
            adiosVar = io.DefineVariable<float>(name, {}, {}, count);
            // adiosVar = io.DefineVariable<float>(name, shape, start, count);
            int16_t data[dataSize];
            float data2[dataSize];
            outputFile << "BlkCnt \t" << numberSteps << std::endl;
            startPuts = Clock::now();
            if (hasSteps)
            {
                for (uint i = 0; i < numberSteps; i++)
                {
                    ncStart[0] = i;

                    startNcGet = Clock::now();
                    variable.getVar(ncStart, ncCount, data);
                    transformValues(name, variable, data, dataSize, data2);
                    endNcGet = Clock::now();

                    ncGetDelta =
                        duration_cast<milliseconds>(endNcGet - startNcGet);
                    ncGetDeltaVector.push_back(ncGetDelta);
                    // std::cout << "ncGetDelta: " << ncGetDelta.count() <<
                    // std::endl;

                    // std::cout << "data2: " << data2[0] << std::endl;
                    startPutBlock = Clock::now();
                    writer.Put<float>(adiosVar, data2, adios2::Mode::Sync);
                    endPutBlock = Clock::now();
                    blockDelta = duration_cast<milliseconds>(endPutBlock -
                                                             startPutBlock);
                    putBlockDelta.push_back(blockDelta);
                }
                writer.PerformPuts();
            }
            else
            {
                startNcGet = Clock::now();
                variable.getVar(data);
                endNcGet = Clock::now();

                ncGetDelta = duration_cast<milliseconds>(endNcGet - startNcGet);
                ncGetDeltaVector.push_back(ncGetDelta);
                // std::cout << "ncGetDelta: " << ncGetDelta.count() <<
                // std::endl;
                if (printVariable)
                    std::cout << "GetType: " << adios2::GetType<float>()
                              << std::endl;
                if (adiosVar)
                {
                    startPutBlock = Clock::now();
                    writer.Put<float>(adiosVar, data2, adios2::Mode::Sync);
                    writer.PerformPuts();
                    endPutBlock = Clock::now();
                    blockDelta = duration_cast<milliseconds>(endPutBlock -
                                                             startPutBlock);
                    putBlockDelta.push_back(blockDelta);
                }
            }
            endPuts = Clock::now();
            putDelta = duration_cast<milliseconds>(endPuts - startPuts);
            putsDelta.push_back(putDelta);
        }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
        adios2::Variable<T> adiosVar;                                          \
        if (isTime)                                                            \
        {                                                                      \
            adiosVar = io.DefineVariable<T>(name, {adios2::LocalValueDim});    \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            adiosVar = io.DefineVariable<T>(name, {}, {}, count);        \
        }                                                                      \
        T data[dataSize];                                                      \
        startPuts = Clock::now();                                              \
        if (hasSteps)                                                          \
        {                                                                      \
            for (uint i = 0; i < numberSteps; i++)                             \
            {                                                                  \
                ncStart[0] = i;                                                \
                                                                               \
                startNcGet = Clock::now();                                     \
                variable.getVar(ncStart, ncCount, data);                       \
                endNcGet = Clock::now();                                       \
                ncGetDelta =                                                   \
                    duration_cast<milliseconds>(endNcGet - startNcGet);        \
                ncGetDeltaVector.push_back(ncGetDelta);                        \
                                                                               \
                startPutBlock = Clock::now();                                  \
                writer.Put<T>(adiosVar, (T *)data, adios2::Mode::Sync);    \
                endPutBlock = Clock::now();                                    \
                blockDelta =                                                   \
                    duration_cast<milliseconds>(endPutBlock - startPutBlock);  \
                putBlockDelta.push_back(blockDelta);                           \
            }                                                                  \
            writer.PerformPuts();                                              \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            startNcGet = Clock::now();                                         \
            variable.getVar(data);                                             \
            endNcGet = Clock::now();                                           \
            ncGetDelta = duration_cast<milliseconds>(endNcGet - startNcGet);   \
            ncGetDeltaVector.push_back(ncGetDelta);                            \
            if (printVariable)                                                 \
                std::cout << "GetType: " << adios2::GetType<T>() << std::endl; \
            if (adiosVar)                                                      \
            {                                                                  \
                startPutBlock = Clock::now();                                  \
                writer.Put<T>(adiosVar, (T *)data, adios2::Mode::Sync);    \
                endPutBlock = Clock::now();                                    \
                blockDelta =                                                   \
                    duration_cast<milliseconds>(endPutBlock - startPutBlock);  \
                putBlockDelta.push_back(blockDelta);                           \
                writer.PerformPuts();                                          \
            }                                                                  \
        }                                                                      \
        endPuts = Clock::now();                                                \
        putDelta = duration_cast<milliseconds>(endPuts - startPuts);           \
        putsDelta.push_back(putDelta);                                         \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

        ++varCount;

        NCcalculateMeanTime(outputFile, putBlockDelta, false);
        NCcalculateMeanTime(outputFile, putsDelta, true);

        size_t ncTime = getTime(outputFile, ncGetDeltaVector);
        // std::cout << "ncTime: " << ncTime << std::endl;
        sumNcGetDeltaVector.push_back(ncTime);

        putsDelta.clear();
        putBlockDelta.clear();
        ncGetDeltaVector.clear();
    }
    writer.Close();
    endOpen = Clock::now();

    size_t sumNcTimes = 0;
    // size_t mean = 0;
    for (auto &time : sumNcGetDeltaVector)
    {
        sumNcTimes += time;
        // std::cout << "delta: " << times.count() << std::endl;
    }

    milliseconds timeOpenClose =
        duration_cast<milliseconds>(endOpen - startOpen);

    size_t sumIOWithoutNc = timeOpenClose.count() - sumNcTimes;

    outputFile << "SumIO \t" << timeOpenClose.count() << std::endl;
    outputFile << "sumIOWithoutNc \t" << sumIOWithoutNc << std::endl;
    outputFile << "-------------------------------\n" << std::endl;

    outputFile << "complete: "<< timeOpenClose.count() << "\t nc: " << sumNcTimes<< " \t without nc: "<< sumIOWithoutNc << std::endl;
    std::cout  << "complete: "<< timeOpenClose.count() << "\t nc: " << sumNcTimes<< " \t without nc: "<< sumIOWithoutNc << std::endl;
    std::cout << sumIOWithoutNc << std::endl;
}

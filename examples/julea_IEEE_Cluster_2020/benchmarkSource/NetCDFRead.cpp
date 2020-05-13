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
#include <iomanip>
#include <iostream>
#include <netcdf>
#include <vector>

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
                     size_t dataSize)
{
    std::cout << "--- transformValues T " << std::endl;

//     auto varAttrMap = variable.getAtts();
//     auto parentGroup = variable.getParentGroup();
//     auto groupAttrMap = parentGroup.getAtts();

//     std::string scaleName = "scale_factor";
//     auto scaleFactor = variable.getAtt(scaleName);

//     std::string offsetName = "add_offset";
//     auto offSet = variable.getAtt(offsetName);

//     double scale;
//     double offset;

//     // T *data2[dataSize];
//     scaleFactor.getValues(&scale);
//     offSet.getValues(&offset);

//     // std::cout << "scale: " << scale << std::endl;
//     // std::cout << "offset: " << offset << std::endl;

//     auto numberElements = dataSize / sizeof(short);
//     // std::cout << "numberElements: " << numberElements << std::endl;
//     auto currentValue = 0;

//     for (int i = 0; i < 3; i++)
//     {
//         // std::cout << "test" << std::endl;
//         // std::cout << "test: " << (short) data[0] << std::endl;
//         currentValue += currentValue;

//         std::string adiosType = "int16_t";
//         // data[i] = data[i] * scale + offset;\

//         if (adiosType == "compound")
//         {
//         }
// #define declare_type(T)                                                        \
//     else if (adiosType == adios2::GetType<T>())                                \
//     {                                                                          \
//         std::cout << "test: " << data[0] << std::endl;                         \
//     }
//         ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
// #undef declare_type
//     }
}

template <>
void transformValues<int16_t>(std::string varName, netCDF::NcVar variable,
int16_t *data, size_t dataSize )
{
    std::cout << "--- transformValues " << std::endl;

    auto varAttrMap = variable.getAtts();
    auto parentGroup = variable.getParentGroup();
    auto groupAttrMap = parentGroup.getAtts();

    std::string scaleName = "scale_factor";
    auto scaleFactor = variable.getAtt(scaleName);

    std::string offsetName = "add_offset";
    auto offSet = variable.getAtt(offsetName);

    double scale;
    double offset;

    // T *data2[dataSize];
    scaleFactor.getValues(&scale);
    offSet.getValues(&offset);

    // std::cout << "scale: " << scale << std::endl;
    // std::cout << "offset: " << offset << std::endl;

    auto numberElements = dataSize / sizeof(short);
    // std::cout << "numberElements: " << numberElements << std::endl;
    auto currentValue = 0;

    for (int i = 0; i < 3; i++)
    {
        // std::cout << "test" << std::endl;
        // std::cout << "test: " << (short) data[0] << std::endl;
        currentValue += currentValue;

        std::string adiosType = "int16_t";
        // data[i] = data[i] * scale + offset;\

        if (adiosType == "compound")
        {
        }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
        std::cout << "test: " << data[0] << std::endl;                         \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
    }
}

/**
 * [NCReadFile description]
 * @param engine          [description]
 * @param ncFileName      [description]
 * @param adiosFileName   [description]
 * @param printDimensions [description]
 * @param printVariable   [description]
 */
void NCReadFile(std::string engine, std::string ncFileName,
                std::string adiosFileName, bool printDimensions,
                bool printVariable)
{
    std::cout << "\n____ Read file ____" << std::endl;
    std::cout << "NetCDF4 file: " << ncFileName << std::endl;
    std::cout << "ADIOS2 file: " << adiosFileName << "\n" << std::endl;
    std::cout << "engine: " << engine << std::endl;

    bool hasSteps = false;
    bool needsTransform = false;
    bool isTime = false;
    size_t varCount = 0;
    size_t dimCount = 0;
    size_t dimsID = 0;
    size_t dimsSize = 0;
    size_t numberSteps = 0;
    size_t dataSize = 1;

    /** ADIOS2 open file ... etc. */
    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Engine writer = io.Open(adiosFileName, adios2::Mode::Write);

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

    auto groupAttrMap = dataFile.getAtts();

    std::cout << "number of attributes: " << groupAttrMap.size() << std::endl;
    for (const auto &attr : groupAttrMap)
    {
        std::string attrName = attr.first;
        netCDF::NcGroupAtt attribute = attr.second;
        std::cout << "group attribute name:" << attrName << std::endl;
    }

    auto varMap = dataFile.getVars();

    /** all variables declared in the nc file */
    for (const auto &var : varMap)
    {

        std::string name = var.first;
        netCDF::NcVar variable = var.second;

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

        if (typeID == NC_SHORT)
        {
            std::cout << "set needsTransform" << std::endl;
            needsTransform = true;
        }
        T *data;
        /** Define and write ADIOS 2 variable */
        if (adiosType == "compound")
        {
        }
        else if((adiosType == "int16_t") && needsTransform)
        {

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
            adiosVar = io.DefineVariable<T>(name, shape, start, count);        \
        }                                                                      \
        T data[dataSize];                                                      \
        if (hasSteps)                                                          \
        {                                                                      \
            for (uint i = 0; i < numberSteps; i++)                             \
            {                                                                  \
                ncStart[0] = i;                                                \
                variable.getVar(ncStart, ncCount, data);                       \
                if (needsTransform)                                            \
                {                                                              \
                    float data2[dataSize];\
                    transformValues(name, variable, data, dataSize);           \
                }                                                              \
                writer.Put<T>(adiosVar, (T *)data, adios2::Mode::Deferred);    \
            }                                                                  \
            writer.PerformPuts();                                              \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            variable.getVar(data);                                             \
            if (needsTransform)                                                \
            {                                                                  \
                transformValues(name, variable, data, dataSize);               \
            }                                                                  \
            if (printVariable)                                                 \
                std::cout << "GetType: " << adios2::GetType<T>() << std::endl; \
            if (adiosVar)                                                      \
            {                                                                  \
                writer.Put<T>(adiosVar, (T *)data, adios2::Mode::Deferred);    \
                writer.PerformPuts();                                          \
            }                                                                  \
        }                                                                      \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

        ++varCount;
    }
    writer.Close();
}
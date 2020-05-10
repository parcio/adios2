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
#include <adios2.h>
#include <iomanip>
#include <iostream>
#include <netcdf>
#include <vector>
#include "NetCDFRead.h"

// std::string mapNCTypeToAdiosType(size_t typeID)
// {
//     const char *type;
//     if (typeID == NC_BYTE)
//     {
//         type = "int8_t";
//     }
//     else if (typeID == NC_CHAR)
//     {
//         type = "int8_t";
//     }
//     else if (typeID == NC_SHORT)
//     {
//         type = "int16_t";
//     }
//     else if (typeID == NC_INT)
//     {
//         type = "int32_t";
//     }
//     else if (typeID == NC_FLOAT)
//     {
//         type = "float";
//     }
//     else if (typeID == NC_DOUBLE)
//     {
//         type = "double";
//     }
//     else if (typeID == NC_UBYTE)
//     {
//     }
//     else if (typeID == NC_USHORT)
//     {
//         type = "uint16_t";
//     }
//     else if (typeID == NC_UINT)
//     {
//         type = "uint32_t";
//     }
//     else if (typeID == NC_INT64)
//     {
//         type = "int64_t";
//     }
//     else if (typeID == NC_UINT64)
//     {
//         type = "uint64_t";
//     }
//     else if (typeID == NC_STRING)
//     {
//         //"std::string (only used for global and local values, not arrays)"
//         // adios2 docu
//         // https://adios2.readthedocs.io/en/latest/components/components.html#variable
//     }
//     else if (typeID == NC_VLEN)
//     {
//         std::cout << "typeID: " << NC_VLEN << " currently not supported!"
//                   << std::endl;
//     }
//     else if (typeID == NC_OPAQUE)
//     {
//         std::cout << "typeID: " << NC_OPAQUE << " currently not supported!"
//                   << std::endl;
//     }
//     else if (typeID == NC_ENUM)
//     {
//         std::cout << "typeID: " << NC_ENUM << " currently not supported!"
//                   << std::endl;
//     }
//     else if (typeID == NC_COMPOUND)
//     {
//         std::cout << "typeID: " << NC_COMPOUND << " currently not supported!"
//                   << std::endl;
//     }
//     return std::string(type);
// }

// void NCReadFile(std::string engine, std::string ncFileName, std::string adiosFileName,
//           bool printDimensions, bool printVariable)
// {
//     std::cout << "\n____ Read file ____" << std::endl;
//     std::cout << "NetCDF4 file: " << ncFileName << std::endl;
//     std::cout << "ADIOS2 file: " << adiosFileName << "\n" << std::endl;
//     std::cout << "engine: " << engine << std::endl;

//     bool hasSteps = false;
//     size_t varCount = 0;
//     size_t dimCount = 0;
//     size_t dimsID = 0;
//     size_t dimsSize = 0;
//     size_t numberSteps = 0;
//     size_t dataSize = 1;

//     /** ADIOS2 open file ... etc. */
//     adios2::ADIOS adios(adios2::DebugON);
//     adios2::IO io = adios.DeclareIO("Output");
//     io.SetEngine(engine);

//     adios2::Engine writer = io.Open(adiosFileName, adios2::Mode::Write);

//     /** netCDF4 open file ... etc. */
//     netCDF::NcFile dataFile;
//     dataFile.open(ncFileName, netCDF::NcFile::read);

//     auto fileDims = dataFile.getDims();

//     if (printDimensions)
//     {

//         std::cout << "number of file dimensions: " << fileDims.size()
//                   << std::endl;
//     }

//     /** all dimensions declared for the nc file */
//     for (const auto &dim : fileDims)
//     {
//         std::string name = dim.first;
//         netCDF::NcDim dimension = dim.second;

//         if (printDimensions)
//         {
//             std::cout << "dimension name: " << name << std::endl;
//         }
//     }

//     auto varMap = dataFile.getVars();

//     /** all variables declared in the nc file */
//     for (const auto &var : varMap)
//     {

//         std::string name = var.first;
//         netCDF::NcVar variable = var.second;

//         netCDF::NcType type = variable.getType();
//         auto typeID = type.getId();
//         auto typeName = type.getName();
//         std::vector<netCDF::NcDim> varDims = variable.getDims();

//         auto varAttrMap = variable.getAtts();

//         adios2::Dims shape;
//         adios2::Dims start;
//         adios2::Dims count;

//         std::vector<size_t> ncStart;
//         std::vector<size_t> ncCount;

//         dimCount = 0;
//         dataSize = 1;
//         hasSteps = 0;

//         if (printVariable)
//         {
//             std::cout << "\n---------------------- variable: " << varCount
//                       << "----------------------" << std::endl;
//             std::cout << "" << name << " - " << type.getName() << ":"
//                       << std::endl;
//         }

//         /** all dimensions for the current variable */
//         for (const auto &dims : varDims)
//         {

//             std::string dimsName = dims.getName();
//             dimsSize = dims.getSize();

//             if (strcmp(dimsName.c_str(), "time") == 0)
//             {
//                 hasSteps = true;
//                 numberSteps = dimsSize;
//                 ncStart.push_back(0);
//                 ncCount.push_back(1);
//             }
//             else
//             {
//                 dataSize = dataSize * dimsSize;

//                 shape.push_back(dimsSize);
//                 start.push_back(0);
//                 count.push_back(dimsSize);

//                 if (hasSteps)
//                 {
//                     ncStart.push_back(0);
//                     ncCount.push_back(dimsSize);
//                 }
//             }

//             if (printVariable)
//             {
//                 std::cout << "\n-- Dim: " << dimCount << std::endl;
//                 std::cout << "Name: " << dims.getName() << std::endl;
//                 std::cout << "getID: " << dims.getId() << std::endl;
//                 std::cout << "size: " << dims.getSize() << std::endl;
//                 std::cout << "isUnlimited: " << dims.isUnlimited() << std::endl;

//                 std::cout << "hasSteps: " << hasSteps << std::endl;
//                 std::cout << "numberSteps: " << numberSteps << std::endl;
//             }
//             ++dimCount;
//         }

//         std::string adiosType = mapNCTypeToAdiosType(typeID);

//         /** Define and write ADIOS 2 variable */
//         if (adiosType == "compound")
//         {
//         }
// #define declare_type(T)                                                        \
//     else if (adiosType == adios2::GetType<T>())                                \
//     {                                                                          \
//         auto var = io.DefineVariable<T>(name, shape, start, count);            \
//         T data[dataSize];                                                      \
//         if (hasSteps)                                                          \
//         {                                                                      \
//             for (uint i = 0; i < numberSteps; i++)                             \
//             {                                                                  \
//                 ncStart[0] = i;                                                \
//                 variable.getVar(ncStart, ncCount, data);                       \
//                 writer.Put<T>(var, (T *)data, adios2::Mode::Deferred);         \
//                 writer.PerformPuts();                                          \
//             }                                                                  \
//         }                                                                      \
//         else                                                                   \
//         {                                                                      \
//             variable.getVar(data);                                             \
//             if (printVariable)                                                 \
//                 std::cout << "GetType: " << adios2::GetType<T>() << std::endl; \
//             if (var)                                                           \
//             {                                                                  \
//                 writer.Put<T>(var, (T *)data, adios2::Mode::Sync);             \
//             }                                                                  \
//         }                                                                      \
//     }
//         ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
// #undef declare_type

//         ++varCount;
//     }
//     writer.Close();
// }

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "_____ Convert an NetCDF file to one of the formats supported "
                 "by the following ADIOS2 engines _____"
              << std::endl;
    std::cout << "use 'bp3' for BP3 format" << std::endl;
    std::cout << "use 'bp4' for BP4 format" << std::endl;
    std::cout << "\nIf compiled accordingly the following formats are also "
                 "available."
              << std::endl;
    std::cout << "use 'hdf5' for HDF5 format" << std::endl;
    std::cout
        << "use 'julea-db' for BP format stored in JULEA database backenend"
        << std::endl;

    std::cout << "--- bpls usage ---" << std::endl;
    std::cout << "\n... 'bpls -D file.bp' to show variable decomposition"
              << std::endl;
    std::cout << "... 'bpls -d file.bp' to dump content of file" << std::endl;
    std::cout << "... 'bpls -d -l file.bp' to dump content of file "
                 "with min/max values"
              << std::endl;
    std::cout << "... 'bpls -d -l file.bp variableName' to dump variable "
                 "with min/max values\n"
              << std::endl;

    try
    {
        // sresa1b_ncar_ccsm3-example.nc = example file from
        // "https://www.unidata.ucar.edu/software/netcdf/examples/files.html"

        // grib2netcdf-webmars-public-svc-blue-004-6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc
        // = dataset from
        // https://apps.ecmwf.int/datasets/data/cera20c/levtype=sfc/type=an/

        // read("bp3", "sresa1b_ncar_ccsm3-example.nc",
             // "sresa1b_ncar_ccsm3-example.bp", true, false);
        // read("bp3",
             // "_grib2netcdf-webmars-public-svc-blue-004-"
             // "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc",
             // "_grib2netcdf-webmars-public-svc-blue-004-"
             // "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.bp",
             // true, false);

        NCReadFile("julea-db", "sresa1b_ncar_ccsm3-example.nc",
             "sresa1b_ncar_ccsm3-example.bp", true, false);
        NCReadFile("julea-db",
             "_grib2netcdf-webmars-public-svc-blue-004-"
             "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc",
             "_grib2netcdf-webmars-public-svc-blue-004-"
             "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.bp",
             true, false);
    }
    catch (std::invalid_argument &e)
    {
        if (rank == 0)
        {
            std::cout << "Invalid argument exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::ios_base::failure &e)
    {
        if (rank == 0)
        {
            std::cout << "System exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::exception &e)
    {
        if (rank == 0)
        {
            std::cout << "Exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    return 0;
}

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
    }
    else if (typeID == NC_OPAQUE)
    {
    }
    else if (typeID == NC_ENUM)
    {
    }
    else if (typeID == NC_COMPOUND)
    {
    }
    return std::string(type);
}

void read(std::string engine, std::string ncFileName, std::string adiosFileName)
{
    std::cout << "\n ncFileName: " << ncFileName << std::endl;
    std::cout << "\n adiosFileName: " << adiosFileName << std::endl;
    std::cout << "engine: " << engine << std::endl;

    bool hasSteps = false;
    size_t varCount = 0;
    size_t dimCount = 0;
    size_t dimsID = 0;
    size_t dimsSize = 0;
    size_t numberSteps = 0;
    size_t dataSize = 1;
    // std::string dimsName;

    /** ADIOS2 open file ... etc. */
    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Engine writer = io.Open(adiosFileName, adios2::Mode::Write);

    /** netCDF4 open file ... etc. */
    netCDF::NcFile dataFile;
    dataFile.open(ncFileName, netCDF::NcFile::read);

    auto fileDims = dataFile.getDims();
    std::cout << "number of dimensions: " << fileDims.size() << std::endl;

    /** all dimensions declared for the file */
    for (const auto &dim : fileDims)
    {
        std::string name = dim.first;
        netCDF::NcDim dimension = dim.second;
        std::cout << "dim name: " << name << std::endl;
    }

    auto varMap = dataFile.getVars();
    /** all variables declared in the file */
    for (const auto &var : varMap)
    {
        std::cout << "\n---------------------- variable: " << varCount
                  << "----------------------" << std::endl;
        std::string name = var.first;
        netCDF::NcVar variable = var.second;

        netCDF::NcType type = variable.getType();
        auto typeID = type.getId();
        auto typeName = type.getName();
        std::vector<netCDF::NcDim> varDims = variable.getDims();

        std::cout << "" << name << " - " << type.getName() << ":" << std::endl;

        // auto adiosVarName = g_strdup_printf("Variable_%lu", varCount);
        auto adiosVarName = "variable_" + std::to_string(varCount);
        std::cout << " " << adiosVarName << std::endl;

        auto varAttrMap = variable.getAtts();

        adios2::Dims shape;
        adios2::Dims start;
        adios2::Dims count;

        std::vector<size_t> ncStart;
        std::vector<size_t> ncCount;

        dimCount = 0;
        dataSize = 1;
        hasSteps = 0;

        /** all dimensions for the current variable */
        for (const auto &dims : varDims)
        {
            std::cout << "-- Dim: " << dimCount << std::endl;
            std::cout << "Name: " << dims.getName() << std::endl;
            std::cout << "getID: " << dims.getId() << std::endl;
            std::cout << "size: " << dims.getSize() << std::endl;
            std::cout << "isUnlimited: " << dims.isUnlimited() << std::endl;

            std::string dimsName = dims.getName();
            // dimsID = dims.getId();
            dimsSize = dims.getSize();

            if (strcmp(dimsName.c_str(), "time") == 0)
            {
                hasSteps = true;
                numberSteps = dimsSize;
                std::cout << "---- HAS STEPS --- " << std::endl;
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

            ++dimCount;
        }

        std::cout << "numberSteps: " << numberSteps << std::endl;

        std::string adiosType = mapNCTypeToAdiosType(typeID);
                // writer.BeginStep();                                            \

        if (adiosType == "compound")
        {
        }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
            auto var = io.DefineVariable<T>(name, shape, start, count);    \
        T data[dataSize];                                                      \
        if (hasSteps)                                                          \
        {                                                                      \
            for (uint i = 0; i < numberSteps; i++)                             \
            {                                                                  \
                ncStart[0] = i;                                                \
                variable.getVar(ncStart, ncCount, data);                       \
                writer.Put<T>(var, (T *)data, adios2::Mode::Deferred);             \
                writer.PerformPuts();\
            }                                                                  \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            variable.getVar(data);                                             \
            std::cout << "GetType: " << adios2::GetType<T>() << std::endl;     \
            if (var)                                                           \
            {                                                                  \
                writer.Put<T>(var, (T *)data, adios2::Mode::Sync);             \
            }                                                                  \
        }                                                                      \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
                // writer.EndStep();                                              \

        ++varCount;
    }
    writer.Close();
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... Convert nc file to bp/jv/jb ... " << std::endl;

    try
    {
        // example file from
        // "https://www.unidata.ucar.edu/software/netcdf/examples/files.html"
        // read("julea-db", "sresa1b_ncar_ccsm3-example.nc");
        // read("julea-db",
        // "_grib2netcdf-webmars-public-svc-blue-004-6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc");
        // read("bp3", "sresa1b_ncar_ccsm3-example.nc",
        // "sresa1b_ncar_ccsm3-example.bp");
        read("bp3",
             "_grib2netcdf-webmars-public-svc-blue-004-"
             "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc",
             "_grib2netcdf-webmars-public-svc-blue-004-"
             "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.bp");
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

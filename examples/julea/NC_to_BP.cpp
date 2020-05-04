#include <adios2.h>
#include <iomanip>
#include <iostream>
#include <netcdf>
#include <vector>

std::string mapNCTypeToAdiosType(size_t typeID)
{
    char *type;
    if (typeID == NC_BYTE)
    {
    }
    else if (typeID == NC_CHAR)
    {
    }
    else if (typeID == NC_SHORT)
    {
    }
    else if (typeID == NC_INT)
    {
    }
    else if (typeID == NC_FLOAT)
    {
        type = "float";
    }
    else if (typeID == NC_DOUBLE)
    {
    }
    else if (typeID == NC_UBYTE)
    {
        // io->DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_USHORT)
    {
        // io->DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_UINT)
    {
        // io->DefineVariable<double>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_INT64)
    {
        // io->DefineVariable<int64_t>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_UINT64)
    {
        // io->DefineVariable<uint64_t>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_STRING)
    {
        // io->DefineVariable<std::string>(varName, {}, {}, {Nglobal});
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

void defineVariable(adios2::IO *io, std::string varName, size_t typeID,
                    adios2::Dims shape, adios2::Dims start, adios2::Dims count)
{
    const size_t Nglobal = 2;

    if (typeID == NC_BYTE)
    {
    }
    else if (typeID == NC_CHAR)
    {
    }
    else if (typeID == NC_SHORT)
    {
    }
    else if (typeID == NC_INT)
    {
    }
    else if (typeID == NC_FLOAT)
    {
        std::cout << "--- REACHED FLOAT CASE " << std::endl;
        // auto var = io->DefineVariable<float>(varName, shape, start, count);
        auto var =
            io->DefineVariable<float>(varName, {128, 256}, {0, 0}, {128, 256});
    }
    else if (typeID == NC_DOUBLE)
    {
        auto var = io->DefineVariable<double>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_UBYTE)
    {
        // io->DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_USHORT)
    {
        // io->DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_UINT)
    {
        io->DefineVariable<double>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_INT64)
    {
        io->DefineVariable<int64_t>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_UINT64)
    {
        io->DefineVariable<uint64_t>(varName, {}, {}, {Nglobal});
    }
    else if (typeID == NC_STRING)
    {
        io->DefineVariable<std::string>(varName, {}, {}, {Nglobal});
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
}

void read(std::string engine, std::string fileName, std::string adiosFileName)
{
    std::cout << "\nfileName: " << fileName << std::endl;
    std::cout << "engine: " << engine << std::endl;
    bool hasSteps;
    size_t varCount = 0;
    size_t dimCount = 0;
    size_t dimsSize = 0;
    size_t numberSteps = 0;
    size_t dimsID = 0;
    // std::string dimsName;

    size_t dataSize;
    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Engine writer = io.Open(adiosFileName, adios2::Mode::Write);

    netCDF::NcFile dataFile;
    dataFile.open(fileName, netCDF::NcFile::read);
    auto fileDims = dataFile.getDims();
    std::cout << "number of dimensions: " << fileDims.size() << std::endl;
    for (const auto &dim : fileDims)
    {
        std::string name = dim.first;
        netCDF::NcDim dimension = dim.second;
        std::cout << "dim name: " << name << std::endl;
        // std::cout << "-- Dim: " << dimCount << std::endl;
        //     std::cout << "Name: " << dim.getName() << std::endl;
        //     std::cout << "getID: " << dim.getId() << std::endl;
        //     std::cout << "size: " << dim.getSize() << std::endl;

        // dimsName = dim.getName();
        // dimsID =  dim.getId();
        // dimsSize =  dim.getSize();
    }

    auto varMap = dataFile.getVars();
    for (const auto &var : varMap)
    {
        std::cout << "\n---------------------- variable: " << varCount
                  << "----------------------" << std::endl;
        std::string name = var.first;
        netCDF::NcVar variable = var.second;

        netCDF::NcType type =
            variable
                .getType(); // dataFile.getType(name,netCDF::NcGroup::Location::All);
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

        dimCount = 0;
        dataSize = 1;
        for (const auto &dims : varDims)
        {
            std::cout << "-- Dim: " << dimCount << std::endl;
            std::cout << "Name: " << dims.getName() << std::endl;
            std::cout << "getID: " << dims.getId() << std::endl;
            std::cout << "size: " << dims.getSize() << std::endl;

            std::string dimsName = dims.getName();
            dimsID = dims.getId();
            dimsSize = dims.getSize();

            if (strcmp(dimsName.c_str(), "time") == 0)
            {
                hasSteps = true;
                numberSteps = dims.getSize();
                std::cout << "---- HAS STEPS --- " << std::endl;
                break;
            }
            std::cout << "dataSize: " << dataSize << std::endl;
            /** e.g. add lat and lon */
            shape.push_back(dims.getSize());
            dataSize = dataSize * dims.getSize();
            count.push_back(dims.getSize());

            ++dimCount;
        }

        if (dimCount == 1)
        {
        }
        std::cout << "dataSize: " << dataSize << std::endl;
        std::cout << "shapeSize: " << shape.size() << std::endl;
        std::cout << "shape: " << shape[0] << std::endl;
        std::cout << "count: " << count[0] << std::endl;
        // std::cout << "shape: " << shape[1] << std::endl;
        // shape.size() should be = dimCount (resp. time)
        // defineVariable(&io, adiosVarName, typeID, start, shape, count);

        // if (varCount == 0)
        // {
        // float data[dataSize];
        // variable.getVar(data);

        // for (int i = 0; i < dataSize; i++)
        // {
        //     // std::cout << "i:" << i << " data:" << std::fixed
        //     // << std::setprecision(6) << data[i] << std::endl;
        // }
        // defineVariable(&io, adiosVarName, typeID, shape, {}, {128, 256});
        // mapNCTypeToAdiosType(typeID, &adiosType);
        std::string adiosType = mapNCTypeToAdiosType(typeID);
        // defineVariable(&io, adiosVarName, typeID, shape, {}, shape);

        // auto var2 = io.InquireVariable<float>(adiosVarName);

        // writer.Put<float>(var2, data, adios2::Mode::Sync);
        // const std::string
        // adiosType(io.InquireVariableType(adiosVarName));

        // const std::string adiosType = "float";

        // DefineVariable<float>(varName, {128, 256}, {0, 0}, {128, 256});

        if (adiosType == "compound")
        {
        }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
        if (dimCount == 1)                                                     \
        {                                                                      \
            std::cout << "only one dimension " << std::endl;\
            auto varTest = io.DefineVariable<T>(adiosVarName, {128}, {}, {128});  \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            auto varTest =                                                     \
                io.DefineVariable<T>(adiosVarName, shape, {0, 0}, shape);      \
        }                                                                      \
        T data[dataSize];                                                      \
        variable.getVar(data);                                                 \
        std::cout << "GetType: " << adios2::GetType<T>();                      \
        std::cout << "type: " << adiosType << std::endl;                       \
        auto var = io.InquireVariable<T>(adiosVarName);                        \
        if (var)                                                               \
        {                                                                      \
            writer.Put<T>(var, (T *)data, adios2::Mode::Sync);                 \
        }                                                                      \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

        writer.Close();
        // }
        // inquire variable
        // get data
        // first dimension = time -> begin step .. end step
        // put variable
        ++varCount;
        // break;
    }
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
        read("bp3", "sresa1b_ncar_ccsm3-example.nc",
             "sresa1b_ncar_ccsm3-example.bp");
        // read("bp3", "_grib2netcdf-webmars-public-svc-blue-004-"
        // "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc");
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

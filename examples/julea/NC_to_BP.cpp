#include <adios2.h>
#include <iostream>
#include <netcdf>
#include <vector>

void read(std::string engine, std::string fileName)
{
    std::cout << "\nfileName: " << fileName << std::endl;
    std::cout << "engine: " << engine << std::endl;

    size_t varCount = 0;
    const size_t Nglobal = 2;
    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    netCDF::NcFile dataFile;
    dataFile.open(fileName, netCDF::NcFile::read);

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

        std::cout << "" << name << " - " << type.getName() << ":" << std::endl;

        // auto adiosVarName = g_strdup_printf("Variable_%lu", varCount);
        auto adiosVarName = "variable_" + std::to_string(varCount);
        std::cout << " " << adiosVarName << std::endl;

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
            auto var =
                io.DefineVariable<float>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_DOUBLE)
        {
            auto var =
                io.DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_UBYTE)
        {
            // io.DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_USHORT)
        {
            // io.DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_UINT)
        {
            io.DefineVariable<double>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_INT64)
        {
            io.DefineVariable<int64_t>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_UINT64)
        {
            io.DefineVariable<uint64_t>(adiosVarName, {}, {}, {Nglobal});
        }
        else if (typeID == NC_STRING)
        {
            io.DefineVariable<std::string>(adiosVarName, {}, {}, {Nglobal});
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

        //inquire variable
        //get data
        //first dimension = time -> begin step .. end step
        //put variable
        ++varCount;
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
        read("bp3", "sresa1b_ncar_ccsm3-example.nc");
        read("bp3", "_grib2netcdf-webmars-public-svc-blue-004-"
                    "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc");
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

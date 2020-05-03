#include <adios2.h>
#include <iostream>
#include <netcdf>
#include <vector>
// #include <glib.h>
//
// auto numberGroups = netCDF::NcGroup::getGroupCount(location);
// auto groupMap = netCDF::NcGroup::getGroups();
// auto varMap = netCDF::NcGroup::getVars(&location);

// auto varMap1 = netCDF::NcGroup::getVars(location);
// auto varMap2 = netCDF::NcGroup::getVars(dataFile);
// auto varMap3 = netCDF::NcGroup::getVars(dataFile.Location);
// auto varMap4 = netCDF::NcGroup::getVars(dataFile.Location());
// auto varMap5 = netCDF::NcGroup::getVars(dataFile.Location::Current);

// auto varMap1 = netCDF::NcGroup::getVars(location2);
// auto varMap2 = netCDF::NcGroup::getVars(dataFile2);
// auto varMap3 = netCDF::NcGroup::getVars(dataFile2.Location);
// auto varMap4 = netCDF::NcGroup::getVars(dataFile2.Location());
// auto varMap5 = netCDF::NcGroup::getVars(dataFile2.Location::All);
// auto varMap6 = netCDF::NcGroup::getVars(netCDF::NcGroup::Location::All);
// auto varMap7 = netCDF::NcGroup::getVars();
// auto varMap8 = netCDF::NcGroup::getVars(5);
// auto varMap9 = netCDF::NcGroup::getVars((netCDF::NcGroup::Location::All)5);

void read(std::string engine, std::string fileName)
{
    std::cout << "fileName: " << fileName << std::endl;
    std::cout << "engine: " << engine << std::endl;
    // read in nc file
    // get all variable names

    // for every name
    // get data
    // first dimension = time -> begin step .. end step
    // Open the file.

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
        std::string name = var.first;
        netCDF::NcVar variable = var.second;
        std::cout << "-- name:" << name << std::endl;

        netCDF::NcType type =
            variable
                .getType(); // dataFile.getType(name,netCDF::NcGroup::Location::All);
        auto typeID = type.getId();
        auto typeName = type.getName();

        std::cout << "-- type:" << type.getName() << std::endl;
        std::cout << "--- type ID: " << typeID << std::endl;

        // auto adiosVarName = g_strdup_printf("Variable_%lu", varCount);
        auto adiosVarName = "variable_" + std::to_string(varCount);
        std::cout << "adiosVarName: " << adiosVarName << std::endl;

        const std::string varType = "double";
        // adios2::Variable<varType> var0 = io.DefineVariable<varType>("v0",
        // {},{},{Nglobal});

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

        // if (typeID = )
        // if(typeID = 3)
        // {
        // 		auto test = io.DefineVariable<int32_t>(adiosVarName,
        // {},{},{Nglobal});

        // }
        // else if(typeID = 4)
        // {
        // 		auto test = io.DefineVariable<float>(adiosVarName,
        // {},{},{Nglobal});

        // }
        // switch(typeID)
        // {
        // 	case NC_BYTE:
        // 	std::cout << "test " << std::endl;
        // 		break;
        // 	case NC_CHAR:
        // 		// auto var = io.DefineVariable<char>(adiosVarName,
        // {},{},{Nglobal}); 	std::cout << "test2 " << std::endl; 		break; 	case
        // NC_SHORT: 		auto var = io.DefineVariable<int8_t>(adiosVarName,
        // {},{},{Nglobal}); 		break; 	case NC_INT: 		auto var =
        // io.DefineVariable<int32_t>(adiosVarName, {},{},{Nglobal}); 		break;
        // 	case NC_FLOAT:
        // 		auto var = io.DefineVariable<float>(adiosVarName,
        // {},{},{Nglobal}); 		break;
        // }

        ++varCount;
    }
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... SimpleStepTest ... " << std::endl;
    std::cout << "... Only one process ... " << std::endl;

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

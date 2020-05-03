#include <iostream>
#include <vector>
#include <adios2.h>
#include <netcdf>

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
	//read in nc file
	//get all variable names

	//for every name
		//get data
		//first dimension = time -> begin step .. end step
	// Open the file.

     netCDF::NcFile dataFile;
     dataFile.open(fileName, netCDF::NcFile::read);

     auto varMap = dataFile.getVars();

     size_t varCount = 0;
     for(const auto &var : varMap)
     {
     	std::string name = var.first;
     	netCDF::NcVar variable = var.second;
     	std::cout << "-- name:" << name << std::endl;

     	netCDF::NcType type = dataFile.getType(name,netCDF::NcGroup::Location::All);
     	auto typeID = type.getId();

     	netCDF::NcType testType(3);
     	auto typeID2 = testType.getId();
     	// auto typeName = type.getName();
     	// std::cout << "-- type:" << type.getName() << std::endl;
     	std::cout << "--- type ID: " << typeID << std::endl;
     	std::cout << "--- type ID2: " << typeID2 << std::endl;

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
    	//example file from "https://www.unidata.ucar.edu/software/netcdf/examples/files.html"
    	read("julea-db", "sresa1b_ncar_ccsm3-example.nc");
    	read("julea-db", "_grib2netcdf-webmars-public-svc-blue-004-6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc");
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

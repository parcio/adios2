#include <iostream>
#include <vector>
#include <adios2.h>
#include <netcdf>


void read(std::string engine, std::string fileName)
{
	//read in nc file
	//get all variable names

	//for every name
		//get data
		//first dimension = time -> begin step .. end step
	// Open the file.
     netCDF::NcFile dataFile(fileName, netCDF::NcFile::read);
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... SimpleStepTest ... " << std::endl;
    std::cout << "... Only one process ... " << std::endl;

    try
    {
    	read("julea-db", "sresa1b_ncar_ccsm3-example.nc");
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

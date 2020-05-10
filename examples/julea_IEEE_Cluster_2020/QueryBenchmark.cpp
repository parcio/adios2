/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application demonstrating some of the query possibilities enabled by the
 * JULEA database engine.
 *
 *  Created on: May 10, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include <adios2.h>
#include <iomanip>
#include <iostream>
#include <vector>
#include "AdiosQuery.h"
#include "JuleaQuery.h"

void Test(std::string fileName, std::string variableName)
{

}


int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... Convert nc file to bp/jv/jb ... " << std::endl;
    std::string fileName = "sresa1b_ncar_ccsm3-example.nc";
    std::string fileName2 = "_grib2netcdf-webmars-public-svc-blue-004-"
                            "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc";


    //TODO:
    // parameter: directory with files to read
    // parameter: number of files to read
    // parameter: engine name

    try
    {
        // TODO: include option to pass a directory and this application then
        // figures out which files are inside and need to be included in query
        AdiosReadMinMax(fileName2, "t2m");
        JuleaReadMinMax(fileName2, "t2m");
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

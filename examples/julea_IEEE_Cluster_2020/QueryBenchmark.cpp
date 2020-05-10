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
// #include <getopt.h>
#include "AdiosQuery.h"
#include "JuleaQuery.h"
#include <unistd.h>
#include <vector>

void showUsage(std::string name)
{
    //TODO: print usage infos
}

void showInformation()
{
    //TODO: print infos about program
}

int main(int argc, char *argv[])
{
    int rank = 0;
    size_t opt;

    std::string fileName = "sresa1b_ncar_ccsm3-example.nc";
    std::string fileName2 = "_grib2netcdf-webmars-public-svc-blue-004-"
                            "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc";

    std::string path; //can be file or directory
    std::string engineName; //valid engines: bp3, bp4, julea-db, julea-kv
    size_t numberFilesToRead;
    size_t scenario; //0 = both, 1 Adios, 2 Julea

    std::cout << "argc: " << argc << std::endl;

    while ((opt = getopt(argc, argv, "d:c:n:s:")) != -1)
    {
        switch (opt)
        {
        case 'd':
            path = optarg;
            std::cout << "d: " << path << std::endl;
            break;
        case 'c':
            numberFilesToRead = atoi(optarg);
            std::cout << "c: " << numberFilesToRead << std::endl;
            break;
        case 'n':
            engineName = optarg;
            std::cout << "n: " << engineName << std::endl;
            break;
        case 's':
            scenario = atoi(optarg);
            std::cout << "s: " << scenario << std::endl;
            break;
        default: /* '?' */
            exit(EXIT_FAILURE);
        }
        if (optind > argc)
        {
            std::cout << "optind: " << optind << std::endl;
            std::cerr << "Expected argument after options" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    try
    {
        switch (scenario)
        {
            case 0:
                //JULEA + ADIOS
            case 1:
                AdiosReadMinMax(fileName2, "t2m");
                break;
            case 2:
                JuleaReadMinMax(fileName2, "t2m");
                break;
        }
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

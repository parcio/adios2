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
#include <cstring>
#include <iomanip>
#include <iostream>
// #include <getopt.h>
#include "benchmarkSource/AdiosRead.h"
#include "benchmarkSource/JuleaRead.h"
#include "benchmarkSource/NetCDFRead.h"
#include <unistd.h>
#include <vector>

void showUsage()
{
    std::cout
        << "Usage: \n"
        << "-d: directory where the files are located\n"
        << "-c: number of files to read\n"
        << "-p: percentage of variables to read; e.g. 50 -> 50%\n"
        << "-n: name of engine to use\n"
        << "    'bp3'\n"
        << "    'bp4'\n"
        << "    if compiled accordingly:\n"
        << "    'julea-db'\n"
        << "    'julea-kv'\n"
        << "    'hdf5'\n"
        << "-s: scenario that will be run: \n"
        << "    0: all (NetCDF4 to ADIOS 2, Read contiguous, Query)\n"
        << "    1: NetCDF4 to ADIOS 2\n"
        << "    2: Read contiguous -> read everything up to specified "
           "percentage "
           "of variables \n"
        << "    3: Read random -> read certain variables/steps/blocks in a "
           "random "
           "order \n"
        << "    4: Query -> directly works on JULEA interfaces not on ADIOS2 \n"
        << std::endl;
}

void showInformation()
{
    // TODO: print infos about program
}

int main(int argc, char *argv[])
{
    int rank = 0;

    bool verbose = 0;
    bool adios = 0;
    bool julea = 0;
    int8_t opt; //must be signed to work with while loop
    uint8_t percentVarsToRead;
    uint8_t scenario; // 0 = both, 1 Adios, 2 Julea
    size_t numberFilesToRead;
    const char *name;

    std::string path;       // can be file or directory
    std::string engineName; // valid engines: bp3, bp4, julea-db, julea-kv

    std::string fileName = "sresa1b_ncar_ccsm3-example.nc";
    std::string fileName2 = "_grib2netcdf-webmars-public-svc-blue-004-"
                            "6fe5cac1a363ec1525f54343b6cc9fd8-ICkLWm.nc";

    try
    {
        while ((opt = getopt(argc, argv, "hivd:c:p:n:s:")) != -1)
        {
            switch (opt)
            {
            case 'h':
                showUsage();
                break;
            case 'i':
                showInformation();
                break;
            case 'v':
                verbose = 1;
                break;
            case 'd':
                path = optarg;
                break;
            case 'c':
                numberFilesToRead = atoi(optarg);
                break;
            case 'p':
                percentVarsToRead = atoi(optarg);
                break;
            case 'n':
                engineName = optarg;
                name = engineName.c_str();
                if ((strcmp(name, "bp3") == 0) || (strcmp(name, "bp4") == 0) ||
                    (strcmp(name, "hdf5") == 0))
                {
                    adios = 1;
                }
                else if ((strcmp(name, "julea-db") == 0) ||
                         (strcmp(name, "julea-kv") == 0))
                {
                    julea = 1;
                }
                else
                {
                    std::cout << "Engine type is not supported by benchmark!"
                              << std::endl;
                    exit(EXIT_FAILURE);
                }
                break;
            case 's':
                scenario = atoi(optarg);
                break;
            default: /* '?' */
                std::cout << "default: exit failure" << std::endl;
                exit(EXIT_FAILURE);
            }
            if (optind > argc)
            {
                std::cout << "optind: " << optind << std::endl;
                std::cerr << "Expected argument after options" << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        if (verbose)
        {
            std::cout << "passed parameters:\n"
                      << "\nd: directory = " << path
                      << "\nc: number of files to read = " << numberFilesToRead
                      << "\np: percentage of variables to read = "
                      << percentVarsToRead
                      << "\nn: engine name = " << engineName
                      << "\ns: scenario = " << (uint) scenario << "\n"
                      << std::endl;
        }

        switch (scenario)
        {
        case 0:
            // JULEA + ADIOS
        case 1:
            // NCReadFile();
            break;
        case 2:
            // read
            if (adios)
            {
                std::cout << "Reached" << std::endl;
                AdiosRead(name, path, numberFilesToRead, percentVarsToRead);
            }
            else if (julea)
            {
                JuleaRead(name, path, numberFilesToRead, percentVarsToRead);
            }
            break;
        case 3:
            // read random
            JuleaReadMinMax(fileName2, "t2m");
            break;
        case 4:
            // query
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

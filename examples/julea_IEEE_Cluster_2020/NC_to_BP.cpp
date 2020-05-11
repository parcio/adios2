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
#include <iomanip>
#include <iostream>
#include <unistd.h>

#include "benchmarkSource/NetCDFRead.h"

void showUsage()
{
    std::cout
        << "Usage: \n"
        << "-h: help\n"
        << "-i: additional information for engines and bpls (optional)\n"
        << "-v: print provided parameters (optional)\n"
        << "-d: NetCDF file (from)\n"
        << "-f: ADIOS file (to) \n"
        << "-n: name of engine to use\n"
        << "    'bp3'\n"
        << "    'bp4'\n"
        << "    if compiled accordingly:\n"
        << "    'julea-db'\n"
        << "    'julea-kv'\n"
        << "    'hdf5'\n"
        << "-o: print output: (optional)\n"
        << "    0: none\n"
        << "    1: print file dimensions\n"
        << "    2: print variables\n"
        << "    3: print both\n"
        << std::endl;
}

void showGeneralInformation()
{
    std::cout
              << "\n_______________________ NC to BP "
                 "_____________________________________\n" <<
    " Converts NetCDF4 variable to ADIOS 2 variables \n and writes them to one of the following formats:\n"
              << " 'bp3' for BP3 format\n"
              << " 'bp4' for BP4 format\n"
              << "\n If compiled accordingly the following formats are also "
                 "available.\n"

             << " 'hdf5' for HDF5 format\n"
             << " 'julea-db' to store ADIOS 2 metadata in JULEA database backenend\n   and data in the object store backend\n"
             << " 'julea-jv' to store ADIOS 2 metadata in JULEA key-value backenend\n   and data in the object store backend\n"
        << std::endl;

    std::cout
              << "\n----------------------- bpls usage "
                 "-----------------------------------\n"
              << " 'bpls -D file.bp' to show variable decomposition\n"
              << " 'bpls -d file.bp' to dump content of file\n"
              << " 'bpls -d -l file.bp' to dump content of file "
                 "with min/max values\n"
              << " 'bpls -d -l file.bp variableName' to dump variable "
                 "with min/max values\n"
              << std::endl;
}

void showFileInfo()
{
    std::cout << "\n----------------------- NetCDF4 files "
                 "---------------------------------\n"
              << "  Example NetCDF files can be found at    \n"
                 "    https://www.unidata.ucar.edu/software/netcdf/examples/"
                 "files.html\n"
              << "  For larger NetCDF files from ECWMF check:    \n"
              << "    https://apps.ecmwf.int/datasets\n"
              << std::endl;
}

int main(int argc, char *argv[])
{
    int rank = 0;
    bool printVariable = 0;
    bool printDimensions = 0;
    bool verbose = 0;

    int8_t opt;
    uint8_t output; // print output option

    std::string NetCDFFile; // NetCDF file to read in
    std::string engineName; // valid engines: bp3, bp4, julea-db, julea-kv
    std::string ADIOSFile;  // name for new file

    try
    {
        while ((opt = getopt(argc, argv, "hivo:d:f:n:")) != -1)
        {
            switch (opt)
            {
            case 'h':
                showUsage();
                break;
            case 'i':
                showGeneralInformation();
                showFileInfo();
                break;
            case 'v':
                verbose = 1;
                break;
            case 'o':
                output = atoi(optarg);
                switch (output)
                {
                case 0:
                    printDimensions = 0;
                    printVariable = 0;
                    break;
                case 1:
                    printDimensions = 1;
                    break;
                case 2:
                    printVariable = 1;
                    break;
                case 3:
                    printDimensions = 1;
                    printVariable = 1;
                    break;
                }
                break;
            case 'd':
                NetCDFFile = optarg;
                break;
            case 'f':
                ADIOSFile = optarg;
                break;
            case 'n':
                engineName = optarg;
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
        if(verbose)
        {
             std::cout << "passed parameters:\n"
                << "d: NetCDF4 file = " << NetCDFFile
                << "\nf: ADIOS2 file = " << ADIOSFile
                << "\nn: engine name = " << engineName
                << "\no: print output = " << output << std::endl;
        }
        NCReadFile(engineName, NetCDFFile, ADIOSFile, printDimensions,
                   printVariable);
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

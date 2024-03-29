/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Settings.cpp
 *
 *  Created on: Dec 2017
 *      Author: Norbert Podhorszki
 */

#include "JuleaQuerySettings.h"

#include <cstdlib>
#include <errno.h>
#include <iomanip>
#include <iostream>
#include <stdexcept>

static unsigned int convertToUint(std::string varName, char *arg)
{
    char *end;
    long retval = std::strtol(arg, &end, 10);
    if (end[0] || errno == ERANGE)
    {
        throw std::invalid_argument("Invalid value given for " + varName +
                                    ": " + std::string(arg));
    }
    if (retval < 0)
    {
        throw std::invalid_argument("Negative value given for " + varName +
                                    ": " + std::string(arg));
    }
    return static_cast<unsigned int>(retval);
}

JuleaQuerySettings::JuleaQuerySettings(int argc, char *argv[], int rank,
                                       int nproc)
: rank{rank}
{
    if (argc < 4)
    {
        throw std::invalid_argument("Not enough arguments");
    }
    this->nproc = (unsigned int)nproc;

    m_Configfile = argv[1];
    m_Inputfile = argv[2];
    steps = convertToUint("steps", argv[3]);
    // outputfile = argv[3];
    // npx = convertToUint("N", argv[4]);
    // npy = convertToUint("M", argv[5]);

    // npx = convertToUint("N", argv[3]);
    // npy = convertToUint("M", argv[4]);
    // if (npx * npy != static_cast<unsigned int>(this->nproc))
    // {
    //     throw std::invalid_argument("N*M must equal the number of
    //     processes");
    // }
    // posx = rank % npx;
    // posy = rank / npx;

    if (rank == 0)
    {
        // std::cout << "\n# --- Query time in mikroseconds ---" << std::endl;
        // std::cout << "# configfile: " << configfile << "\n";
        // std::cout << "# inputfile: " << inputfile << "\n";
        // // std::cout << "# outputfile: " << outputfile << "\n";
        // std::cout << "# MPI_Comm_size: " << nproc << "\n";
        // std::cout << "# N \t M" << std::endl;
        // std::cout << npx << " \t " << npy << "\n" << std::endl;ZZ

        // std::cout << "# N: " << npx << "\n";
        // std::cout << "# M: " << npy << "\n";
        // std::cout << "# \n --- measured times (mikroseconds): ---\n";
        // std::cout << "# get: \t\t right before and right after GET; in case
        // of "
        //              "deferred I/O nothing is actually read\n";
        // std::cout
        //     << "# step: \t right before and right after ENDSTEP; this is "
        //        "where deferred reads happen\n";
        // std::cout << "# read: \t\t right before GET and right after ENDSTEP;
        // "
        //              "complete read time for deferred reads\n";
        // std::cout << "# Mean \t Rank 0" << std::endl;
    }
}

// void JuleaQuerySettings::DecomposeArray(int gndx, int gndy)
// {
//     // 2D decomposition of global array reading
//     size_t ndx = gndx / npx;
//     size_t ndy = gndy / npy;
//     size_t offsx = ndx * posx;
//     size_t offsy = ndy * posy;
//     if (posx == npx - 1)
//     {
//         // right-most processes need to read all the rest of rows
//         ndx = gndx - ndx * (npx - 1);
//     }

//     if (posy == npy - 1)
//     {
//         // bottom processes need to read all the rest of columns
//         ndy = gndy - ndy * (npy - 1);
//     }
//     readsize.push_back(ndx);
//     readsize.push_back(ndy);
//     offset.push_back(offsx);
//     offset.push_back(offsy);

//     // std::cout << "rank " << rank << " reads 2D slice " << ndx << " x " <<
//     ndy
//     // << " from offset (" << offsx << "," << offsy << ")" << std::endl;
// }

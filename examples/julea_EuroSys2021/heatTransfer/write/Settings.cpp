/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Settings.cpp
 *
 *  Created on: Feb 2017
 *      Author: Norbert Podhorszki
 */

#include "Settings.h"

#include <chrono>
#include <errno.h>

#include <cstdlib>
#include <fstream>
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

Settings::Settings(int argc, char *argv[], int rank, int nproc) : rank{rank}
{
    if (argc < 9)
    {
        throw std::invalid_argument("Not enough arguments");
    }
    this->nproc = (unsigned int)nproc;

    configfile = argv[1];
    outputfile = argv[2];
    npx = convertToUint("N", argv[3]);
    npy = convertToUint("M", argv[4]);
    ndx = convertToUint("nx", argv[5]);
    ndy = convertToUint("ny", argv[6]);
    steps = convertToUint("steps", argv[7]);
    iterations = convertToUint("iterations", argv[8]);

    if (npx * npy != this->nproc)
    {
        throw std::invalid_argument("N*M must equal the number of processes");
    }

    // calculate global array size and the local offsets in that global space
    gndx = npx * ndx;
    gndy = npy * ndy;
    posx = rank % npx;
    posy = rank / npx;
    offsx = posx * ndx;
    offsy = posy * ndy;

    // determine neighbors
    if (posx == 0)
        rank_up = -1;
    else
        rank_up = rank - 1;

    if (posx == npx - 1)
        rank_down = -1;
    else
        rank_down = rank + 1;

    if (posy == 0)
        rank_left = -1;
    else
        rank_left = rank - npx;

    if (posy == npy - 1)
        rank_right = -1;
    else
        rank_right = rank + npx;

    // std::ofstream timeOutput("heatTransfer-Output.txt");
    // if (timeOutput.is_open())
    // {
    //     if (rank == 0)
    //     {
    //         timeOutput << "configfile: " << configfile << "\n";
    //         timeOutput << "outputfile: " << outputfile << "\n";
    //         timeOutput << "N: " << npx << "\n";
    //         timeOutput << "M: " << npy << "\n";
    //         timeOutput << "nx: " << ndx << "\n";
    //         timeOutput << "ny: " << ndy << "\n";
    //         timeOutput << "steps: " << steps << "\n";
    //         timeOutput << "iterations: " << iterations << "\n";

    //         timeOutput << "\n --- measured times (mikroseconds): ---\n";
    //         timeOutput
    //             << "put: \t\t right before and right after PUT; in case of "
    //                "deferred I/O nothing is actually written\n";
    //         timeOutput
    //             << "endstep: \t right before and right after ENDSTEP; this is
    //             "
    //                "where deferred writes happen\n";
    //         timeOutput
    //             << "write: \t\t right before PUT and right after ENDSTEP; "
    //                "complete write time for deferred writes\n";
    //         timeOutput << "\n--- Write time in mikroseconds ---" <<
    //         std::endl; timeOutput.close();
    //     }
    // }

    if (rank == 0)
    {
        std::cout << "\n# --- Write time in mikroseconds ---" << std::endl;
        std::cout << "# configfile: " << configfile << "\n";
        std::cout << "# outputfile: " << outputfile << "\n";
        std::cout << "# MPI_Comm_size: " << nproc << "\n";
        // std::cout << "# N: " << npx << "\n";
        // std::cout << "# M: " << npy << "\n";
        // std::cout << "# nx: " << ndx << "\n";
        // std::cout << "# ny: " << ndy << "\n";
        // std::cout << "# steps: " << steps << "\n";
        // std::cout << "# iterations: " << iterations << "\n";
        // std::cout << "# \n --- measured times (mikroseconds): ---\n";
        // std::cout << "# put: \t\t right before and right after PUT; in case
        // of "
        //              "deferred I/O nothing is actually written\n";
        // std::cout << "# step: \t right before and right after ENDSTEP; this
        // is "
        //              "where deferred writes happen\n";
        // std::cout << "# write: \t\t right before PUT and right after ENDSTEP;
        // "
        //              "complete write time for deferred writes\n";

        std::cout << "# N \t M \t ny \t ny \t steps \t iterations" << std::endl;
        std::cout << npx << " \t " << npy << " \t " << ndx << " \t " << ndy
                  << " \t " << steps << " \t " << iterations << "\n"
                  << std::endl;

        std::cout << "# Mean \t Rank 0" << std::endl;
    }
}

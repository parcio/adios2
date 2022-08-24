/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * main.cpp
 *
 * Recreates heat_transfer.f90 (Fortran) ADIOS tutorial example in C++
 *
 * Created on: Feb 2017
 *     Author: Norbert Podhorszki
 *
 */
#include <mpi.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include <julea-dai.h>
#include <julea.h>

#include "HeatTransfer.h"
#include "IO.h"
#include "Settings.h"

void printUsage()
{
    std::cout << "Usage: heatTransfer  config   output  N  M   nx  ny   steps "
                 "iterations\n"
              << "  config: XML config file to use\n"
              << "  output: name of output data file/stream\n"
              << "  N:      number of processes in X dimension\n"
              << "  M:      number of processes in Y dimension\n"
              << "  nx:     local array size in X dimension per processor\n"
              << "  ny:     local array size in Y dimension per processor\n"
              << "  steps:  the total number of steps to output\n"
              << "  iterations: one step consist of this many iterations\n\n";
}

void SetupDAI(std::string projectNamespace, std::string fileName)
{
    j_dai_init(projectNamespace.c_str());

    // j_dai_create_project_namespace(projectNamespace.c_str());

    j_dai_add_tag_d(projectNamespace.c_str(), "ColderThanMinus12", fileName.c_str(), "T",
                    J_DAI_GRAN_BLOCK, J_DAI_STAT_MAX,
                    J_DAI_OP_LT, -12.0);
    j_dai_pc_stat(
        projectNamespace.c_str(), "computeAllForT", fileName.c_str(), "T", J_DAI_GRAN_BLOCK,
        (JDAIStatistic)(J_DAI_STAT_MIN | J_DAI_STAT_MAX | J_DAI_STAT_MEAN |
                        J_DAI_STAT_SUM | J_DAI_STAT_VAR),
        0);
    j_dai_pc_ic(projectNamespace.c_str(), fileName.c_str(), "T",
                (JDAIClimateIndex)(J_DAI_CI_SU | J_DAI_CI_FD | J_DAI_CI_ID |
                                   J_DAI_CI_TR));
    //  j_dai_pc_ic(
        // projectNamespace.c_str(), fileName.c_str(), "P",
        // (JDAIClimateIndex)(J_DAI_CI_PR1 | J_DAI_CI_PR10 | J_DAI_CI_PR20));
    j_dai_compute_stats_combined(projectNamespace.c_str(), fileName.c_str(),
                                 "T");
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    /* When writer and reader is launched together with a single mpirun command,
       the world comm spans all applications. We have to split and create the
       local 'world' communicator mpiHeatTransferComm for the writer only.
       When writer and reader is launched separately, the mpiHeatTransferComm
       communicator will just equal the MPI_COMM_WORLD.
     */

    int wrank, wnproc;
    MPI_Comm_rank(MPI_COMM_WORLD, &wrank);
    MPI_Comm_size(MPI_COMM_WORLD, &wnproc);

    const unsigned int color = 1;
    MPI_Comm mpiHeatTransferComm;
    MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &mpiHeatTransferComm);

    int rank, nproc;
    MPI_Comm_rank(mpiHeatTransferComm, &rank);
    MPI_Comm_size(mpiHeatTransferComm, &nproc);

    try
    {
        double timeStart = MPI_Wtime();
        Settings settings(argc, argv, rank, nproc);
        HeatTransfer ht(settings);

        if (wrank == 0)
        {
            SetupDAI("Thesis_eval", settings.outputfile);
        }

        IO io(settings, mpiHeatTransferComm);

        ht.init(false);
        // ht.printT("Initialized T:", mpiHeatTransferComm);
        ht.heatEdges();
        ht.exchange(mpiHeatTransferComm);
        // ht.printT("Heated T:", mpiHeatTransferComm);

        io.write(0, ht, settings, mpiHeatTransferComm);

        if (rank == 0)
        {
                std::cout << "\n# Mean \t Sdev \t Rank 0" << std::endl;
        }

        for (unsigned int t = 1; t <= settings.steps; ++t)
        {
            // std::cout << "settings.steps: " << settings.steps << std::endl;
            if (rank == 0)
            {
                // std::cout << "Step " << t << ":\n";
            }

            for (unsigned int iter = 1; iter <= settings.iterations; ++iter)
            {
                ht.iterate();
                ht.exchange(mpiHeatTransferComm);
                ht.heatEdges();
            }

            io.write(t, ht, settings, mpiHeatTransferComm);

        }
        MPI_Barrier(mpiHeatTransferComm);

        double timeEnd = MPI_Wtime();
        if (rank == 0)
            std::cout << "Total runtime = " << timeEnd - timeStart << "s\n";
    }
    catch (std::invalid_argument &e) // command-line argument errors
    {
        std::cout << e.what() << std::endl;
        printUsage();
    }
    catch (std::ios_base::failure &e) // I/O failure (e.g. file not found)
    {
        std::cout << "I/O base exception caught\n";
        std::cout << e.what() << std::endl;
    }
    catch (std::exception &e) // All other exceptions
    {
        std::cout << "Exception caught\n";
        std::cout << e.what() << std::endl;
    }
    // j_dai_fini();

    MPI_Finalize();
    return 0;
}

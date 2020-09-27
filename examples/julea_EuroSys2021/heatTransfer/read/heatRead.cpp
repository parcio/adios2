/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * IO_ADIOS2.cpp
 *
 *  Created on: Nov 2017
 *      Author: Norbert Podhorszki
 *
 */
#include <mpi.h>

#include "adios2.h"

#include <chrono>
using namespace std::chrono;
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <math.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <math.h>

#include "PrintDataStep.h"
#include "ReadSettings.h"

void printUsage()
{
    std::cout << "Usage: heatRead  config  input  output N  M \n"
              << "  config:  XML config file to use\n"
              << "  input:   name of input data file/stream\n"
              << "  output:  name of output data file/stream\n"
              << "  N:       number of processes in X dimension\n"
              << "  M:       number of processes in Y dimension\n\n";
}

void Compute(const std::vector<double> &Tin, std::vector<double> &Tout,
             std::vector<double> &dT, bool firstStep)
{
    /* Compute dT and
     * copy Tin into Tout as it will be used for calculating dT in the
     * next step
     */
    if (firstStep)
    {
        for (size_t i = 0; i < dT.size(); ++i)
        {
            dT[i] = 0;
            Tout[i] = Tin[i];
        }
    }
    else
    {
        for (size_t i = 0; i < dT.size(); ++i)
        {
            dT[i] = Tout[i] - Tin[i];
            Tout[i] = Tin[i];
        }
    }
}

void printElements(std::string engineType, std::vector<double> Tin)
{
    std::ofstream outFile;
    outFile.open(engineType + "-readOutput.txt");
    std::cout << "engine type: " << engineType << std::endl;
    // outFile.open(inIO.EngineType() + "-readOutput.txt");
    // std::cout << "engine type: " << inIO.EngineType() << std::endl;
    /*
     * Print every element in Tin
     */
    double sum = 0;
    int i = 0;
    for (auto &el : Tin)
    {
        sum += el;
        if (i % 10 == 0)
        {
            std::cout << std::endl;
            outFile << std::endl;
        }
        std::cout << el << " ";
        outFile << el << " ";
        i++;
    }
    std::cout << "\n"
              << "sum: " << sum << std::endl;
    outFile << "\n"
            << "sum: " << sum << std::endl;
}

void printDurations(
    std::chrono::time_point<std::chrono::high_resolution_clock> stopGet,
    std::chrono::time_point<std::chrono::high_resolution_clock> startGet,
    std::chrono::time_point<std::chrono::high_resolution_clock> stopEndStep,
    std::chrono::time_point<std::chrono::high_resolution_clock> startEndStep,
    int rank, int nproc)
{
    // right before and right after PUT; in case of deferred I/O nothing is
    // actually written
    auto durationGet = duration_cast<microseconds>(stopGet - startGet);

    // right before and right after ENDSTEP; this is where deferred writes
    // happen
    auto durationEndStep =
        duration_cast<microseconds>(stopEndStep - startEndStep);

    // right before GET and right after ENDSTEP; complete write time for
    // deferred reads
    auto durationRead = duration_cast<microseconds>(stopEndStep - startGet);

    // durationGet = duration_cast<microseconds>(stopGet - startGet);
    // durationEndStep =
    // duration_cast<microseconds>(stopEndStep - startEndStep);
    durationRead = duration_cast<microseconds>(stopEndStep - startGet);

    size_t readSum = 0;
    size_t readSquareSum = 0;
    size_t readMean = 0;
    size_t readSdev = 0;
    size_t read = durationRead.count();
    size_t readSquare = read * read;

    MPI_Reduce(&read, &readSum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&readSquare, &readSquareSum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    readMean = readSum / nproc;
    readSdev = sqrt(readSquareSum / nproc - (readMean * readMean));
    if (rank == 0)
    {
        std::cout << readMean;
        std::cout << "\t" << readSdev;
        std::cout << "\t" << read;
        // std::cout << "get: \t rank: \t" << rank << "\t"
        //           << durationGet.count() << "\n"
        //           << "step: \t rank: \t" << rank << "\t"
        //           << durationEndStep.count() << "\n"
        //           << "read: \t rank: \t" << rank << "\t"
        //           << durationRead.count() << std::endl;
        for (int i = 1; i < nproc; i++)
        {
            // size_t get, step, read;
            MPI_Status status;

            // MPI_Recv(&get, 1, MPI_LONG, i, 0, MPI_COMM_WORLD,
            // &status); MPI_Recv(&step, 1, MPI_LONG, i, 0,
            // MPI_COMM_WORLD, &status);
            MPI_Recv(&read, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &status);
            // std::cout << "get: \t rank: \t" << i << "\t" << get <<
            // "\n"
            //           << "step: \t rank: \t" << i << "\t" << step
            //           << "\n"
            //           << "read: \t rank: \t" << i << "\t" << read
            //           << std::endl;
            std::cout << "\t " << read;
        }
        std::cout << std::endl;
    }
    else
    {
        // size_t get = durationGet.count();
        // size_t step = durationEndStep.count();
        // size_t read = durationRead.count();
        // MPI_Send(&get, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
        // MPI_Send(&step, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&read, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
    }
    // MPI_Barrier(mpiReaderComm);
    // if (rank == 0)
    // {
    //     std::cout << "--- Ending step: " << step << " \t---\n\n"
    //               << std::endl;
    // }
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    /* When writer and reader is launched together with a single mpirun command,
       the world comm spans all applications. We have to split and create the
       local 'world' communicator for the reader only.
       When writer and reader is launched separately, the mpiReaderComm
       communicator will just equal the MPI_COMM_WORLD.
     */

    int wrank, wnproc;
    MPI_Comm_rank(MPI_COMM_WORLD, &wrank);
    MPI_Comm_size(MPI_COMM_WORLD, &wnproc);

    const unsigned int color = 2;
    MPI_Comm mpiReaderComm;
    MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &mpiReaderComm);

    int rank, nproc;
    MPI_Comm_rank(mpiReaderComm, &rank);
    MPI_Comm_size(mpiReaderComm, &nproc);

    try
    {
        ReadSettings settings(argc, argv, rank, nproc);
        adios2::ADIOS ad(settings.configfile, mpiReaderComm);

        // Define method for engine creation
        // 1. Get method def from config file or define new one

        adios2::IO inIO = ad.DeclareIO("readerInput");
        if (!inIO.InConfigFile())
        {
            // if not defined by user, we can change the default settings
            // BPFile is the default engine
            inIO.SetEngine("BP3");
            inIO.SetParameters({{"num_threads", "1"}});

            // ISO-POSIX file output is the default transport (called "File")
            // Passing parameters to the transport
            inIO.AddTransport("File", {{"verbose", "4"}});
        }

        adios2::IO outIO = ad.DeclareIO("readerOutput");

        adios2::Engine reader =
            inIO.Open(settings.inputfile, adios2::Mode::Read, mpiReaderComm);

        std::vector<double> Tin;
        std::vector<double> Tout;
        std::vector<double> dT;
        adios2::Variable<double> vTin;
        adios2::Variable<double> vTout;
        adios2::Variable<double> vdT;
        adios2::Engine writer;
        bool firstStep = true;
        int step = 0;

        auto startEndStep = high_resolution_clock::now();
        auto stopEndStep = high_resolution_clock::now();

        auto startGet = high_resolution_clock::now();
        auto stopGet = high_resolution_clock::now();

        while (true)
        {
            adios2::StepStatus status =
                reader.BeginStep(adios2::StepMode::Read);
            if (status != adios2::StepStatus::OK)
            {
                break;
            }

            // Variable objects disappear between steps so we need this every
            // step
            vTin = inIO.InquireVariable<double>("T");

            if (!vTin)
            {
                std::cout << "Error: NO variable T found. Unable to proceed. "
                             "Exiting. "
                          << std::endl;
                break;
            }

            if (firstStep)
            {
                // Promise that we are not going to change the variable sizes
                // nor add new variables
                reader.LockReaderSelections();

                unsigned int gndx = static_cast<unsigned int>(vTin.Shape()[0]);
                unsigned int gndy = static_cast<unsigned int>(vTin.Shape()[1]);

                if (rank == 0)
                {
                    std::cout << "# gndx       = " << gndx << std::endl;
                    std::cout << "# gndy       = " << gndy << std::endl;
                }

                settings.DecomposeArray(gndx, gndy);
                Tin.resize(settings.readsize[0] * settings.readsize[1]);
                Tout.resize(settings.readsize[0] * settings.readsize[1]);
                dT.resize(settings.readsize[0] * settings.readsize[1]);

                /* Create output variables and open output stream */
                vTout = outIO.DefineVariable<double>(
                    "T", {gndx, gndy}, settings.offset, settings.readsize);
                vdT = outIO.DefineVariable<double>(
                    "dT", {gndx, gndy}, settings.offset, settings.readsize);
                writer = outIO.Open(settings.outputfile, adios2::Mode::Write,
                                    mpiReaderComm);

                // MPI_Barrier(mpiReaderComm); // sync processes just for stdout
                if (rank == 0)
                {
                    std::cout << "\n# Mean \t Sdev \t Rank 0" << std::endl;
                }
            }

            if (!rank)
            {
                // std::cout << "Processing step " << step << std::endl;
            }

            // Create a 2D selection for the subset
            vTin.SetSelection(
                adios2::Box<adios2::Dims>(settings.offset, settings.readsize));

            startGet = high_resolution_clock::now();

            // Arrays are read by scheduling one or more of them
            // and performing the reads at once
            reader.Get<double>(vTin, Tin.data());
            // reader.Get<double>(vTin, Tin.data(), adios2::Mode::Sync);
            /*printDataStep(Tin.data(), settings.readsize.data(),
                          settings.offset.data(), rank, step); */
            stopGet = high_resolution_clock::now();

            startEndStep = high_resolution_clock::now();
            reader.EndStep();
            stopEndStep = high_resolution_clock::now();

            /* Compute dT from current T (Tin) and previous T (Tout)
             * and save Tin in Tout for output and for future computation
             */
            Compute(Tin, Tout, dT, firstStep);

            /* Output Tout and dT */
            // writer.BeginStep();

            // if (vTout)
            //     writer.Put<double>(vTout, Tout.data());
            // if (vdT)
            //     writer.Put<double>(vdT, dT.data());
            // writer.EndStep();

            printDurations(stopGet, startGet, stopEndStep, startEndStep, rank,
                           nproc);
            // printElements(inIO.EngineType(), Tin);

            step++;
            firstStep = false;
        }
        reader.Close();
        if (writer)
            writer.Close();
        // outFile.close();
    }
    catch (std::invalid_argument &e) // command-line argument errors
    {
        std::cout << e.what() << std::endl;
        printUsage();
    }

    MPI_Finalize();
    return 0;
}

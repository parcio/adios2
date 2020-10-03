/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * main.cpp
 *
 * Analyses heatTransfer Output
 *
 * Created on: Oct 2020
 *     Author: Kira Duwe
 *
 */
#include <mpi.h>

#include <fstream>
#include <mpi.h>

#include "adios2.h"

#include <chrono>
using namespace std::chrono;
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <math.h>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "QueryPrintDataStep.h"
#include "QuerySettings.h"

void computeDistancesFromMean(const std::vector<double> &values,
                              std::vector<double> &TdifferencesMean,
                              double Tmean)
{
    std::cout << "compute distance" << std::endl;
    for (size_t i = 0; i < values.size(); ++i)
    {
        TdifferencesMean[i] = std::abs(values[i] - Tmean);
        std::cout << "Difference: " << TdifferencesMean[i] << " = " << values[i]
                  << " - " << Tmean << std::endl;
    }
}

void ComputeMean(const std::vector<double> &Tin, double &Mean)
{
    // TODO: why is there dt.Size in read?
    auto sum = std::accumulate(Tin.begin(), Tin.end(), 0);
    Mean = sum / Tin.size();
    std::cout << "mean: " << Mean << std::endl;
}

void printUsage()
{
    std::cout << "Usage: heatQuery  config  input  output N  M \n"
              << "  config:  XML config file to use\n"
              << "  input:   name of input data file/stream\n"
              << "  output:  name of output data file/stream\n"
              << "  N:       number of processes in X dimension\n"
              << "  M:       number of processes in Y dimension\n\n";
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

    // const unsigned int color = 1;
    const unsigned int color = 2;
    MPI_Comm mpiQueryComm;
    MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &mpiQueryComm);

    int rank, nproc;
    MPI_Comm_rank(mpiQueryComm, &rank);
    MPI_Comm_size(mpiQueryComm, &nproc);

    try
    {
        double timeStart = MPI_Wtime();

        QuerySettings settings(argc, argv, rank, nproc);
        std::cout << "configfile: " << settings.configfile << std::endl;
        adios2::ADIOS ad(settings.configfile, mpiQueryComm);

        adios2::IO inIO = ad.DeclareIO("readerInput");
        adios2::IO outIO = ad.DeclareIO("readerOutput");

        std::cout << "This is called -- 1" << std::endl;
        MPI_Barrier(mpiQueryComm); //who knows maybe this helps for mariadb...

        adios2::Engine reader =
            inIO.Open(settings.inputfile, adios2::Mode::Read, mpiQueryComm);
        std::cout << "This is called -- 2" << std::endl;

        std::vector<double> Tin;
        // std::vector<double> Tout;
        double Tmean;
        // std::vector<double> means;
        double diffMeanMax;
        double diffMeanMin;
        std::vector<double> TdifferencesMean;
        adios2::Variable<double> vTin;
        // adios2::Variable<double> vTout;
        // adios2::Variable<double> vdT;
        adios2::Engine writer;
        bool firstStep = true;
        int step = 0;

        auto startEndStep = high_resolution_clock::now();
        auto stopEndStep = high_resolution_clock::now();

        auto startGet = high_resolution_clock::now();
        auto stopGet = high_resolution_clock::now();

        while (true)
        {
            if (firstStep)
            {
                std::cout << "rank: " << rank << std::endl;
            }
            adios2::StepStatus status =
                reader.BeginStep(adios2::StepMode::Read);
            if (status != adios2::StepStatus::OK)
            {
                break;
            }

            MPI_Barrier(mpiQueryComm); // sync to avoid race conditions?!

            // Variable objects disappear between steps so we need this every
            // step
            vTin = inIO.InquireVariable<double>("T");

            if (!vTin)
            {
                // std::cout << "Error: NO variable T found. Unable to proceed.
                // " "Exiting. "
                // << std::endl;
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
                //         Tout.resize(settings.readsize[0] *
                //         settings.readsize[1]); dT.resize(settings.readsize[0]
                //         * settings.readsize[1]);

                //         /* Create output variables and open output stream */
                //         vTout = outIO.DefineVariable<double>(
                //             "T", {gndx, gndy}, settings.offset,
                //             settings.readsize);
                //         vdT = outIO.DefineVariable<double>(
                //             "dT", {gndx, gndy}, settings.offset,
                //             settings.readsize);
                //         writer = outIO.Open(settings.outputfile,
                //         adios2::Mode::Write,
                //                             mpiReaderComm);

                MPI_Barrier(mpiQueryComm); // sync processes just for stdout
                if (rank == 0)
                {
                    std::cout << "\n# Mean \t Sdev \t Rank 0" << std::endl;
                }
            }

            //     if (!rank)
            //     {
            //         // std::cout << "Processing step " << step << std::endl;
            //     }

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

            std::vector<double> testValues = {1, 2, 3, 4, 5, 6, 7, 42};
            std::vector<double> testDifferences;
            testDifferences.resize(testValues.size());
            double TmeanTest = 0;
            ComputeMean(testValues, TmeanTest);
            computeDistancesFromMean(testValues, testDifferences, TmeanTest);

            ComputeMean(Tin, Tmean);
            double Tmin = vTin.Min();
            double Tmax = vTin.Max();

            computeDistancesFromMean(Tin, TdifferencesMean, Tmean);
            diffMeanMin = Tmean - Tmin;
            diffMeanMax = Tmax - Tmean;

            // TODO: output/answer: coordinates of areas which where the coldest
            // but heated up the fastest

            //     /* Output Tout and dT */
            //     writer.BeginStep();

            //     if (vTout)
            //         writer.Put<double>(vTout, Tout.data());
            //     if (vdT)
            //         writer.Put<double>(vdT, dT.data());
            //     writer.EndStep();

            //     printDurations(stopGet, startGet, stopEndStep, startEndStep,
            //     rank,
            //                    nproc);
            //     // printElements(inIO.EngineType(), Tin);

            //     step++;
            //     firstStep = false;
        }
        reader.Close();
        if (writer)
            writer.Close();
        // outFile.close();
        MPI_Barrier(mpiQueryComm);

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

    MPI_Finalize();
    return 0;
}

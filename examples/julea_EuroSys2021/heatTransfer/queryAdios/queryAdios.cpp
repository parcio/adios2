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

#include <algorithm>
#include <chrono>
using namespace std::chrono;
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <iterator>
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
    // std::cout << "compute distance" << std::endl;
    for (size_t i = 0; i < values.size(); ++i)
    {
        // std::cout << "i = " << i << std::endl;
        TdifferencesMean[i] = std::abs(values[i] - Tmean);

        if (TdifferencesMean[i] < (Tmean * 0.1))
        {
            // std::cout << "i " << i << std::endl;
        }
        // std::cout << "Difference: " << TdifferencesMean[i] << " = " <<
        // values[i]
        // << " - " << Tmean << std::endl;
    }
}

void ComputeMean(const std::vector<double> &Tin, double &Mean)
{
    // TODO: why is there dt.Size in read?
    // auto sum = std::accumulate(Tin.begin(), Tin.end(), 0);
    // std::cout << "sum: " << sum << std::endl;
    double sum = 0;
    for (size_t i = 0; i < Tin.size(); ++i)
    {
        sum += Tin[i];
    }

    Mean = sum / (double)Tin.size();
    // std::cout << "mean: " << Mean << std::endl;
    // std::cout << "Tin size: " << Tin.size() << std::endl;
}

void printElements(std::vector<double> Tin)
{
    // std::ofstream outFile;
    // outFile.open(engineType + "-readOutput.txt");
    // std::cout << "engine type: " << engineType << std::endl;
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
            // outFile << std::endl;
        }
        std::cout << el << " ";
        // outFile << el << " ";
        i++;
    }
    std::cout << "\n"
              << "sum: " << sum << std::endl;
    // outFile << "\n"
    // << "sum: " << sum << std::endl;
}

void printUsage()
{
    std::cout << "Usage: heatQuery  config  input  output N  M \n"
              << "  config:  XML config file to use\n"
              << "  input:   name of input data file/stream\n"
              << std::endl;
    // << "  output:  name of output data file/stream\n"
    // << "  N:       number of processes in X dimension\n"
    // << "  M:       number of processes in Y dimension\n\n";
}

void printQueryDurations(
    std::chrono::time_point<std::chrono::high_resolution_clock> stopEndStep,
    std::chrono::time_point<std::chrono::high_resolution_clock> startGet,
    std::chrono::time_point<std::chrono::high_resolution_clock> stopCompute,
    std::chrono::time_point<std::chrono::high_resolution_clock> startCompute,
    std::chrono::time_point<std::chrono::high_resolution_clock> stopAnalysis,
    std::chrono::time_point<std::chrono::high_resolution_clock> startAnalysis)
{
    // right before GET and right after ENDSTEP; complete write time for
    // deferred reads
    auto durationRead = duration_cast<microseconds>(stopEndStep - startGet);

    auto durationAnalysis =
        duration_cast<microseconds>(stopAnalysis - startAnalysis);
    auto durationCompute =
        duration_cast<microseconds>(stopCompute - startCompute);

    std::cout << durationRead.count() << " \t " << durationCompute.count()
              << " \t " << durationAnalysis.count() << std::endl;
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
        // std::cout << "configfile: " << settings.configfile << std::endl;
        adios2::ADIOS ad(settings.configfile, mpiQueryComm);

        adios2::IO inIO = ad.DeclareIO("readerInput");
        adios2::IO outIO = ad.DeclareIO("readerOutput");

        // std::cout << "This is called -- 1" << std::endl;
        MPI_Barrier(mpiQueryComm); // who knows maybe this helps for mariadb...
        // std::cout << settings.inputfile << std::endl;
        adios2::Engine reader =
            inIO.Open(settings.inputfile, adios2::Mode::Read, mpiQueryComm);
        // std::cout << "This is called -- 2" << std::endl;

        std::vector<double> Tin1;
        std::vector<double> meansTin1;
        std::vector<double> Tin5;
        std::vector<double> meansTin5;
        std::vector<double> diffMeans;
        adios2::Variable<double> vTin;

        adios2::Engine writer;
        bool firstStep = true;
        int step = 0;

        auto startEndStep = high_resolution_clock::now();
        auto stopEndStep = high_resolution_clock::now();

        auto startRead = high_resolution_clock::now();
        auto stopRead = high_resolution_clock::now();
        auto diffRead = stopRead - stopRead;

        auto startCompute = high_resolution_clock::now();
        auto stopCompute = high_resolution_clock::now();
        auto diffCompute = stopCompute - stopCompute;

        auto startAnalysis = high_resolution_clock::now();
        auto stopAnalysis = high_resolution_clock::now();

        vTin = inIO.InquireVariable<double>("T");

        auto blocksInfo = vTin.AllStepsBlocksInfo();

        auto blocksInfo1 = blocksInfo[1];
        auto blocksInfo5 = blocksInfo[5];

        startAnalysis = high_resolution_clock::now();

        for (size_t i = 0; i < blocksInfo1.size(); i++)
        {
            double mean = 0;
            std::vector<double> data;

            vTin.SetStepSelection({1, 1});
            vTin.SetBlockSelection(i);
            data.resize(blocksInfo1[i].Count[0] * blocksInfo1[i].Count[1]);

            startRead = high_resolution_clock::now();
            reader.Get<double>(vTin, data.data(), adios2::Mode::Sync);
            stopRead = high_resolution_clock::now();
            diffRead += stopRead - startRead;

            startCompute = high_resolution_clock::now();
            ComputeMean(data, mean);
            stopCompute = high_resolution_clock::now();
            diffCompute = stopCompute - startCompute;
            // std::cout << "mean: " << mean << std::endl;
            // std::cout << "min: " << blocksInfo1[i].Min << std::endl;
            // std::cout << "max: " << blocksInfo1[i].Max << std::endl;

            meansTin1.push_back(mean);
        }
        std::cout << std::endl;

        for (size_t i = 0; i < blocksInfo5.size(); i++)
        {
            double mean = 0;
            std::vector<double> data;

            vTin.SetStepSelection({5, 1});
            vTin.SetBlockSelection(i);
            data.resize(blocksInfo5[i].Count[0] * blocksInfo5[i].Count[1]);

            startRead = high_resolution_clock::now();
            reader.Get<double>(vTin, data.data(), adios2::Mode::Sync);
            stopRead = high_resolution_clock::now();
            diffRead += stopRead - startRead;

            startCompute = high_resolution_clock::now();
            ComputeMean(data, mean);
            stopCompute = high_resolution_clock::now();
            diffCompute = stopCompute - startCompute;
            // std::cout << "mean: " << mean << std::endl;
            // std::cout << "min: " << blocksInfo5[i].Min << std::endl;
            // std::cout << "max: " << blocksInfo5[i].Max << std::endl;

            meansTin5.push_back(mean);
        }

        diffMeans.resize(meansTin1.size());

        startCompute = high_resolution_clock::now();
        for (size_t i = 0; i < meansTin1.size(); ++i)
        {
            diffMeans[i] = meansTin5[i] - meansTin1[i];
        }

        size_t index =
            std::distance(diffMeans.begin(),
                          std::max_element(diffMeans.begin(), diffMeans.end()));
        stopCompute = high_resolution_clock::now();
        diffCompute = stopCompute - startCompute;

        // std::cout << "index: " << index << "\tdiff: " << diffMeans[index]
        // << std::endl;

        stopAnalysis = high_resolution_clock::now();
        auto durationAnalysis =
            duration_cast<microseconds>(stopAnalysis - startAnalysis);
        auto durationRead = duration_cast<microseconds>(diffRead);
        auto durationCompute = duration_cast<microseconds>(diffCompute);

        std::cout << "\n# Read \t Compute \t Analysis" << std::endl;
        std::cout << durationRead.count() << " \t " << durationCompute.count()
                  << " \t " << durationAnalysis.count() << std::endl;

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

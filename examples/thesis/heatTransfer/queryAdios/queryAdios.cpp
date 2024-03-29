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

#include "AdiosQueryPrintDataStep.h"
#include "AdiosQuerySettings.h"

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

void AdiosQueryAllInRange(std::string fileName, adios2::IO inIO,
                          adios2::Engine reader)
{
    auto tempVar = inIO.InquireVariable<double>("T");

    auto blocksInfoAllSteps = tempVar.AllStepsBlocksInfo();
    std::vector<size_t> blockIDs;

    for (auto blocksInfoPerStep : blocksInfoAllSteps)
    {
        for (auto block : blocksInfoPerStep)
        {
            if ((block.Max < 42) && (block.Min > -42))
            {
                blockIDs.push_back(block.BlockID);
            }
        }
    }
}

// compute mean for every block of every step, return highest mean
void AdiosQueryHighestMean(std::string fileName, adios2::IO inIO,
                           adios2::Engine reader)
{
    double mean = 0;
    uint32_t dataSize = 0;
    std::vector<double> tempData;
    std::vector<double> blockMeans;

    adios2::Variable<double> tempVar = inIO.InquireVariable<double>("T");

    // get global dimensions of variable per step
    for (auto element : tempVar.Shape())
    {
        dataSize *= element;
    }

    std::vector<size_t> blockIDs;

    for (int i = 0; i < tempVar.Steps(); i++)
    {
        auto blocksPerStep = reader.BlocksInfo(tempVar, i);

        for (int j = 0; j < blocksPerStep.size(); j++)
        {
            tempVar.SetStepSelection({i, 1});
            tempVar.SetBlockSelection(j);
            tempData.resize(tempVar.SelectionSize());
            reader.Get<double>(tempVar, tempData.data(), adios2::Mode::Sync);
            ComputeMean(tempData, mean);

            blockMeans.push_back(mean);
        }
    }

    double maxMean = *max_element(blockMeans.begin(), blockMeans.end());
    // std::cout << "maxMean: " << maxMean << "\n";
}

// Find biggest difference in max temperature between step 1 and step 100
void AdiosQueryDrasticLocalChangeInTimeInterval(std::string fileName,
                                                adios2::IO inIO,
                                                adios2::Engine reader)
{
    adios2::Variable<double> tempVar = inIO.InquireVariable<double>("T");

    auto blocksInfo = tempVar.AllStepsBlocksInfo();
    double diff = 0;
    double maxDiff = 0;

    auto blockInfos1 = reader.BlocksInfo(tempVar, 1);
    auto blockInfos100 = reader.BlocksInfo(tempVar, 100);

    if (blockInfos1.size() == blockInfos100.size())
    {
        for (int i = 0; i < blockInfos1.size(); ++i)
        {
            auto result1 = blockInfos1[i].Max;
            auto result100 = blockInfos100[i].Max;
            // std::cout << "result1: " << result1 << "\n";
            // std::cout << "result100: " << result100 << "\n";

            diff = std::abs(result1 - result100);
            if (diff > maxDiff)
            {
                maxDiff = diff;
            }
        }
    }
    // std::cout << "maxDiff: " << maxDiff << "\n";
}

// where is block max T > 40 and sum max P > 20?

/**
 * @brief Find the blockID (=location) of the maximum precipiation block sum,
 * where the maximum block temperature is > 40
 *
 * @param fileName
 * @param inIO
 * @param reader
 */
void AdiosQueryRainTemperatureCombinedSimple(std::string fileName,
                                             adios2::IO inIO,
                                             adios2::Engine reader)
{
    auto tempVar = inIO.InquireVariable<double>("T");
    auto precipVar = inIO.InquireVariable<double>("P");

    std::vector<double> precipData;
    std::vector<double> blockSums;

    double sum = 0;
    double maxSum = 0;

    // index in vector = step; vector content = blockIDs for this step
    std::vector<std::vector<size_t>> blockIDsQueryMet;

    for (int i = 0; i < tempVar.Steps(); i++)
    {
        // std::cout << "i = " << i << "\n";
        auto blockInfos = reader.BlocksInfo(tempVar, i);

        std::vector<size_t> blockIDs;
        blockIDsQueryMet.push_back(blockIDs);

        for (int j = 0; j < blockInfos.size(); j++)
        {
            // std::cout << "j = " << j << "\n";
            if (blockInfos[j].Max > 40)
            {
                blockIDsQueryMet[i].push_back(blockInfos[j].BlockID);
                // std::cout << "blockInfos[j].BlockID: " << blockInfos[j].BlockID << "\n";
                // std::cout << "blockInfos[j].Max:" << blockInfos[j].Max << "\n";
                // std::cout << "max T > 40 \n";
            }
        }
    }

    for (int i = 0; i < blockIDsQueryMet.size(); i++)
    {
        for (int j = 0; j < blockIDsQueryMet[i].size(); j++)
        {
            //TODO: wrong size
            // std::cout << "blockIDsQueryMet.size(): " << blockIDsQueryMet.size() << "\n";
            // std::cout << "blockIDsQueryMet[i].size(): " << blockIDsQueryMet[i].size() << "\n";
            tempVar.SetStepSelection({i, 1});
            tempVar.SetBlockSelection(blockIDsQueryMet[i][j]);

            precipData.resize(precipVar.SelectionSize());
            // std::cout << "selection.size: " << precipVar.SelectionSize() << "\n";
            reader.Get<double>(precipVar, precipData.data(),
                               adios2::Mode::Sync);
            sum = std::accumulate(precipData.begin(), precipData.end(), 0);
            // std::cout << "precip.data size: " << precipData.size() << "\n";
            // for (int n : precipData) {
            //     std::cout << n << ", ";
            // }
            auto minimum = *min_element(precipData.begin(), precipData.end());
            auto maximum = *max_element(precipData.begin(), precipData.end());

            // std::cout << "minium: " << minimum << "\n";
            // std::cout << "maximum: " << maximum << "\n";

            blockSums.push_back(sum);
            // std::cout << "sum: " << sum << "\n";
        }
    }
    if (blockSums.size() > 0)
    {
        maxSum = *max_element(blockSums.begin(), blockSums.end());
        // std::cout << "maxSum: " << maxSum << "\n";
    }
}

void AdiosQueryLowestTemp(std::string fileName, adios2::IO inIO,
                          adios2::Engine reader)
{
}

// how many and which days had max temp below -12?
void AdiosQueryDaysColderThan(std::string fileName, adios2::IO inIO,
                              adios2::Engine reader)
{
}

void AdiosQueryCIDays(std::string fileName, adios2::IO inIO,
                      adios2::Engine reader)
{
}

void AdiosQuery(AdiosQuerySettings::AdiosQueryID queryID, std::string fileName,
                adios2::IO inIO, adios2::Engine reader)
{
    switch (queryID)
    {
    case AdiosQuerySettings::AQUERY_ALL_IN_RANGE:
        AdiosQueryAllInRange(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_HIGHEST_MEAN:
        AdiosQueryHighestMean(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_DRASTIC_LOCAL_CHANGE_IN_TIME:
        AdiosQueryDrasticLocalChangeInTimeInterval(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_RAIN_TEMP_COMBINED:
        AdiosQueryRainTemperatureCombinedSimple(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_LOWEST_TEMP_OVER_FILES:
        // QueryLowestTemp(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_NUMBER_DAYS_COLDER_THAN:
        // AdiosQueryDaysColderThan(fileName, inIO, reader);
        break;
    case AdiosQuerySettings::AQUERY_CI_DAYS:
        // AdiosQueryCIDays(fileName, inIO, reader);
        break;
    }
}

void AdiosSetupQueries(
    std::vector<AdiosQuerySettings::AdiosQueryID> *allQueries)
{
    allQueries->push_back(AdiosQuerySettings::AQUERY_ALL_IN_RANGE);
    allQueries->push_back(AdiosQuerySettings::AQUERY_HIGHEST_MEAN);
    allQueries->push_back(
        AdiosQuerySettings::AQUERY_DRASTIC_LOCAL_CHANGE_IN_TIME);
    allQueries->push_back(AdiosQuerySettings::AQUERY_RAIN_TEMP_COMBINED);
    // allQueries->push_back(AdiosQuerySettings::AQUERY_NUMBER_DAYS_COLDER_THAN);
    // allQueries->push_back(AdiosQuerySettings::AQUERY_CI_DAYS);
    // allQueries->push_back(AdiosQuerySettings::AQUERY_LOWEST_TEMP_OVER_FILES);
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
        AdiosQuerySettings settings(argc, argv, rank, nproc);
        adios2::ADIOS ad(settings.configfile, mpiQueryComm);

        adios2::IO inIO = ad.DeclareIO("readerInput");

        MPI_Barrier(mpiQueryComm);
        adios2::Engine reader =
            inIO.Open(settings.inputfile, adios2::Mode::Read, mpiQueryComm);

        std::vector<AdiosQuerySettings::AdiosQueryID> allQueries;
        AdiosSetupQueries(&allQueries);

        if (rank == 0)
        {
            std::cout << "\n# Read \t Compute \t Analysis" << std::endl;
        }
        auto startAnalysis = high_resolution_clock::now();
        auto stopAnalysis = high_resolution_clock::now();

        // evaluate all post-processing queries
        for (auto element : allQueries)
        {
            startAnalysis = high_resolution_clock::now();
            // startRead = high_resolution_clock::now();

            AdiosQuery(element, settings.inputfile, inIO, reader);
            // ReadQuery(element, settings.m_ProjectNamespace,
            // settings.m_Inputfile);

            // stopRead = high_resolution_clock::now();
            // startCompute = high_resolution_clock::now();

            // ComputeQuery(element, settings.m_ProjectNamespace,
            // settings.m_Inputfile);

            // stopCompute = high_resolution_clock::now();
            stopAnalysis = high_resolution_clock::now();

            // printQueryDurations(stopRead, startRead, stopCompute,
            // startCompute, stopAnalysis, startAnalysis);
            printQueryDurations(stopAnalysis, startAnalysis, stopAnalysis,
                                startAnalysis, stopAnalysis, startAnalysis);
        }
        MPI_Barrier(mpiQueryComm);
        double timeEnd = MPI_Wtime();
        if (rank == 0)
            std::cout << "Total runtime = " << timeEnd - timeStart << "s\n"
                      << std::endl;
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

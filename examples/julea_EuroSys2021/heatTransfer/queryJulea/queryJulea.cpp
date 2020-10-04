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
#include <iterator>
#include <math.h>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "JuleaQueryPrintDataStep.h"
#include "JuleaQuerySettings.h"

#include <julea-db.h>
#include <julea.h>

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
    auto sum = std::accumulate(Tin.begin(), Tin.end(), 0);
    // std::cout << "sum: " << sum << std::endl;
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

void JuleaReadMetadata(std::string fileName, std::string variableName,
                       size_t step, std::vector<double> &means)
{
    size_t err = 0;
    size_t db_length = 0;
    gchar *db_field = NULL;
    JDBType type;
    JDBSchema *schema = NULL;
    JDBEntry *entry = NULL;
    JDBIterator *iterator = NULL;
    JDBSelector *selector = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // schema = j_db_schema_new("adios2", "variable-metadata", NULL);
    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            fileName.c_str(), strlen(fileName.c_str()) + 1,
                            NULL);
    j_db_selector_add_field(selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,
                            variableName.c_str(),
                            strlen(variableName.c_str()) + 1, NULL);
    j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ, &step,
                            sizeof(step), NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    std::string adiosType = "double";
    // std::string minField = "min_float64";
    // std::string maxField = "max_float64";
    std::string meanField = "mean_float64";

    // std::vector<double> minima;
    // std::vector<double> maxima;
    // std::vector<double> means;
    uint32_t *tmpID;

    // setMinMaxString(adiosType.c_str(), minField, maxField, valueField);

    double *min;
    double *max;
    double *mean;
    while (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "_id", &type, (gpointer *)&tmpID,
                                &db_length, NULL);
        // j_db_iterator_get_field(iterator, minField.c_str(), &type,
        //                         (gpointer *)&min, &db_length, NULL);
        // j_db_iterator_get_field(iterator, maxField.c_str(), &type,
        //                         (gpointer *)&max, &db_length, NULL);
        j_db_iterator_get_field(iterator, meanField.c_str(), &type,
                                (gpointer *)&mean, &db_length, NULL);
        std::cout << "_id: " << *tmpID << std::endl;
        // minima.push_back(*min);
        // maxima.push_back(*max);
        means.push_back(*mean);
        // std::cout << "min: " << *min << std::endl;
        // std::cout << "max: " << *max << std::endl;
        std::cout << "mean: " << *mean << std::endl;
    }

    j_db_schema_unref(schema);
    // j_db_iterator_unref(iterator);
    j_db_selector_unref(selector);
    // j_batch_unref(batch);
    // j_db_entry_unref(entry);
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
        JuleaQuerySettings settings(argc, argv, rank, nproc);
        std::cout << settings.inputfile << std::endl;
        std::cout << "steps = " << settings.steps << std::endl;

        std::vector<double> means1;
        std::vector<double> means5;
        std::vector<double> diffMeans;

        // for (size_t i = 0; i < settings.steps; ++i)
        // {
        // std::cout << "\ni: " << i << std::endl;
        JuleaReadMetadata(settings.inputfile, "T", 1, means1);
        JuleaReadMetadata(settings.inputfile, "T", 5, means5);

        diffMeans.resize(means1.size());

        // std::set_difference(means0.begin(), means0.end(), means5.begin(), means5.end(), std::inserter(diffMeans, diffMeans.begin()));
        for (size_t i = 0; i < means1.size(); ++i)
        {
            diffMeans[i] = means5[i] - means1[i];
        }

        size_t index = std::distance(diffMeans.begin(), std::max_element(diffMeans.begin(), diffMeans.end()));
        std::cout << "max_element: " << *std::max_element(diffMeans.begin(), diffMeans.end()) << std::endl;
        // std::cout << "index of block with max difference in mean value between step 0 and step 5. index = " << std::distance(diffMeans.begin(), std::max_element(diffMeans.begin(), diffMeans.end())) << std::endl;
        std::cout << "index of block with max difference in mean value between step 0 and step 5. index = " << index << std::endl;

        // }

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

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
                       size_t step)
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
    schema = j_db_schema_new("adios2", "variable-blockmetadata", NULL);
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
    std::string minField = "min-float64";
    std::string maxField = "max-float64";
    std::string meanField = "mean";
 
    // setMinMaxString(adiosType.c_str(), minField, maxField, valueField);

    std::cout << "minField: " << minField << std::endl;
    std::cout << "adiosType: " << adiosType << std::endl;

    if (adiosType == "compound")
    {
    }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
        T *min;                                                                \
        T *max;                                                                \
        T *mean;                                                                \
        while (j_db_iterator_next(iterator, NULL))                             \
        {                                                                      \
            j_db_iterator_get_field(iterator, minField.c_str(), &type,         \
                                    (gpointer *)&min, &db_length, NULL);       \
            j_db_iterator_get_field(iterator, maxField.c_str(), &type,         \
                                    (gpointer *)&max, &db_length, NULL);       \
        }                                                                      \
    std::cout << "min: " << min << std::endl;                          \
            std::cout << "max: " << max << std::endl;                          \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
            // j_db_iterator_get_field(iterator, meanField.c_str(), &type,         \
            //                         (gpointer *)&mean, &db_length, NULL);       \

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

        for (size_t i = 0; i < settings.steps; ++i)
        {
            JuleaReadMetadata(settings.inputfile, "T", i);
        }

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

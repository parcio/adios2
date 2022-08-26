/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * IO_ADIOS2.cpp
 *
 *  Created on: Feb 2017
 *      Author: Norbert Podhorszki
 */

#include <adios2.h>
#include <chrono>
#include <fstream>

#include <julea-dai.h>
#include <julea.h>

#include <iostream>
#include <math.h>
#include <random>
#include <sstream>
#include <string>
using namespace std::chrono;

#include "IO.h"

adios2::ADIOS ad;
adios2::Engine bpWriter;
adios2::Variable<double> varT;
adios2::Variable<double> varP;
adios2::Variable<unsigned int> varGndx;
// void SetupDAI(std::string projectNamespace, std::string fileName);

IO::IO(const Settings &s, MPI_Comm comm)
{
    m_outputfilename = s.outputfile;

    ad = adios2::ADIOS(s.configfile, comm);

    adios2::IO bpio = ad.DeclareIO("writer");
    // // if (s.rank == 0 && ((bpWriter.Type() == "julea-kv") ||
    // (bpWriter.Type() == "julea-db") ))
    //  if (s.rank == 0 && ((bpio.EngineType() == "julea-kv") ||
    //  (bpio.EngineType() == "julea-db") ))
    //     {
    //         SetupDAI("Thesis_eval", s.outputfile);
    //     }
    if (!bpio.InConfigFile())
    {
        // if not defined by user, we can change the default settings
        // BPFile is the default engine
        bpio.SetEngine("BPFile");
        bpio.SetParameters({{"num_threads", "1"}});

        // ISO-POSIX file output is the default transport (called "File")
        // Passing parameters to the transport
#ifdef _WIN32
        bpio.AddTransport("File", {{"Library", "stdio"}});
#else
        bpio.AddTransport("File", {{"Library", "posix"}});
#endif
    }

    // define T as 2D global array
    varT = bpio.DefineVariable<double>(
        "T",
        // Global dimensions
        {s.gndx, s.gndy},
        // starting offset of the local array in the global space
        {s.offsx, s.offsy},
        // local size, could be defined later using SetSelection()
        {s.ndx, s.ndy});

    if (bpio.EngineType() == "BP3")
    {
        varT.SetMemorySelection({{1, 1}, {s.ndx + 2, s.ndy + 2}});
    }

    // define T as 2D global array
    varP = bpio.DefineVariable<double>(
        "P",
        // Global dimensions
        {s.gndx, s.gndy},
        // starting offset of the local array in the global space
        {s.offsx, s.offsy},
        // local size, could be defined later using SetSelection()
        {s.ndx, s.ndy});

    if (bpio.EngineType() == "BP3")
    {
        varP.SetMemorySelection({{1, 1}, {s.ndx + 2, s.ndy + 2}});
    }

    bpWriter = bpio.Open(m_outputfilename, adios2::Mode::Write, comm);

    // Promise that we are not going to change the variable sizes nor add new
    // variables
    bpWriter.LockWriterDefinitions();
}

IO::~IO() { bpWriter.Close(); }

// void SetupDAI(std::string projectNamespace, std::string fileName)
// {
//     j_dai_init(projectNamespace.c_str());

//     // j_dai_create_project_namespace(projectNamespace.c_str());

//     j_dai_add_tag_d(projectNamespace.c_str(), "ColderThanMinus12",
//     fileName.c_str(), "T",
//                     J_DAI_GRAN_BLOCK, J_DAI_STAT_MAX,
//                     J_DAI_OP_LT, -12.0);
//     j_dai_pc_stat(
//         projectNamespace.c_str(), "computeAllForT", fileName.c_str(), "T",
//         J_DAI_GRAN_BLOCK, (JDAIStatistic)(J_DAI_STAT_MIN | J_DAI_STAT_MAX |
//         J_DAI_STAT_MEAN |
//                         J_DAI_STAT_SUM | J_DAI_STAT_VAR),
//         0);
//     j_dai_pc_ic(projectNamespace.c_str(), fileName.c_str(), "T",
//                 (JDAIClimateIndex)(J_DAI_CI_SU | J_DAI_CI_FD | J_DAI_CI_ID |
//                                    J_DAI_CI_TR));
//     //  j_dai_pc_ic(
//         // projectNamespace.c_str(), fileName.c_str(), "P",
//         // (JDAIClimateIndex)(J_DAI_CI_PR1 | J_DAI_CI_PR10 | J_DAI_CI_PR20));
//     j_dai_compute_stats_combined(projectNamespace.c_str(), fileName.c_str(),
//                                  "T");
// }

void IO::write(int step, const HeatTransfer &ht, const Settings &s,
               MPI_Comm comm)
{

    // example from
    // https://stackoverflow.com/questions/288739/generate-random-numbers-uniformly-over-an-entire-range
    const int rangeFrom = 0;
    // world record per day 1825 mm; here 2 steps per day
    // https://wmo.asu.edu/content/world-greatest-twenty-four-hour-1-day-rainfall
    const int rangeTo = 500;
    std::random_device randDev;
    std::mt19937 generator(randDev());
    std::uniform_int_distribution<int> distrFrom(rangeFrom, 250);
    std::uniform_int_distribution<int> distrTo(251, rangeTo);

    // auto startEndStep = high_resolution_clock::now();
    auto stopEndStep = high_resolution_clock::now();

    auto startPut = high_resolution_clock::now();
    // auto stopPut = high_resolution_clock::now();

    // right before PUT and right after ENDSTEP; complete write time for
    // deferred writes
    auto durationWrite = duration_cast<microseconds>(stopEndStep - startPut);

    // using PutDeferred() you promise the pointer to the data will be intact
    // until the end of the output step.
    // We need to have the vector object here not to destruct here until the end
    // of function.
    // added support for MemorySelection
    if (bpWriter.Type() == "BP3")
    {
        bpWriter.BeginStep();

        int numberElements = (s.ndx + 2) * (s.ndy + 2);
        // double* data = new double[numberElements];
        std::vector<double> data;
        data.reserve(numberElements);

        // to have more variation in the min/max between steps, the distribution
        // range is set randomly each step.
        // low range between 0 - 250;
        std::uniform_int_distribution<int> distr2(distrFrom(generator),
                                                  distrTo(generator));
        for (int i = 0; i < numberElements; ++i)
        {
            data[i] = distr2(generator);
        }

        startPut = high_resolution_clock::now();
        bpWriter.Put<double>(varT, ht.data());
        bpWriter.Put<double>(varP, data.data());

        bpWriter.EndStep();
        stopEndStep = high_resolution_clock::now();
    }
    else
    {
        bpWriter.BeginStep();

        std::vector<double> v = ht.data_noghost();
        int numberElements = v.size();
        // double* data = new double[numberElements];
        std::vector<double> data;
        data.reserve(numberElements);

        std::uniform_int_distribution<int> distr2(distrFrom(generator),
                                                  distrTo(generator));
        for (int i = 0; i < numberElements; ++i)
        {
            data[i] = distr2(generator);
        }

        startPut = high_resolution_clock::now();
        bpWriter.Put<double>(varT, v.data());
        bpWriter.Put<double>(varP, data.data());

        bpWriter.EndStep();
        stopEndStep = high_resolution_clock::now();
    }

    durationWrite = duration_cast<microseconds>(stopEndStep - startPut);
    m_seedStep++;

    size_t writeSum = 0;
    size_t writeSquareSum = 0;
    size_t writeMean = 0;
    size_t writeSdev = 0;
    size_t write = durationWrite.count();
    size_t writeSquare = write * write;

    MPI_Reduce(&write, &writeSum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&writeSquare, &writeSquareSum, 1, MPI_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);

    writeMean = writeSum / s.nproc;
    writeSdev = sqrt(writeSquareSum / s.nproc - (writeMean * writeMean));

    if (s.rank == 0)
    {
        if (step > 0)
        {
            std::cout << writeMean;
            std::cout << "\t " << writeSdev;
            std::cout << "\t " << write;
        }
        for (int i = 1; i < s.nproc; i++)
        {
            MPI_Status status;

            MPI_Recv(&write, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &status);
            if (step > 0)
            {
                std::cout << "\t " << write;
            }
        }
        if (step > 0)
        {
            std::cout << std::endl;
        }
    }
    else
    {
        MPI_Send(&write, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
    }
}

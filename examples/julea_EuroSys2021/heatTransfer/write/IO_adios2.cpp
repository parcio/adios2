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
#include <iostream>
#include <sstream>
#include <string>
using namespace std::chrono;

#include "IO.h"

adios2::ADIOS ad;
adios2::Engine bpWriter;
adios2::Variable<double> varT;
adios2::Variable<unsigned int> varGndx;

IO::IO(const Settings &s, MPI_Comm comm)
{
    m_outputfilename = s.outputfile;

    ad = adios2::ADIOS(s.configfile, comm);

    adios2::IO bpio = ad.DeclareIO("writer");
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

    bpWriter = bpio.Open(m_outputfilename, adios2::Mode::Write, comm);

    // Promise that we are not going to change the variable sizes nor add new
    // variables
    bpWriter.LockWriterDefinitions();
}

IO::~IO() { bpWriter.Close(); }

void IO::write(int step, const HeatTransfer &ht, const Settings &s,
               MPI_Comm comm)
{

    auto startBeginStep = high_resolution_clock::now();
    auto stopBeginStep = high_resolution_clock::now();

    auto startEndStep = high_resolution_clock::now();
    auto stopEndStep = high_resolution_clock::now();

    auto startPut = high_resolution_clock::now();
    auto stopPut = high_resolution_clock::now();

    // right before and right after PUT; in case of deferred I/O nothing is
    // actually written
    auto durationPut = duration_cast<microseconds>(stopPut - startPut);

    // right before and right after ENDSTEP; this is where deferred writes
    // happen
    auto durationEndStep =
        duration_cast<microseconds>(stopEndStep - startEndStep);

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
        startBeginStep = high_resolution_clock::now();
        bpWriter.BeginStep();
        stopBeginStep = high_resolution_clock::now();

        startPut = high_resolution_clock::now();
        bpWriter.Put<double>(varT, ht.data());
        stopPut = high_resolution_clock::now();

        startEndStep = high_resolution_clock::now();
        bpWriter.EndStep();
        stopEndStep = high_resolution_clock::now();
    }
    else
    {
        startBeginStep = high_resolution_clock::now();
        bpWriter.BeginStep();
        stopBeginStep = high_resolution_clock::now();

        std::vector<double> v = ht.data_noghost();

        startPut = high_resolution_clock::now();
        bpWriter.Put<double>(varT, v.data());
        stopPut = high_resolution_clock::now();
        // bpWriter.Put<double>(varT, v.data(), adios2::Mode::Sync);

        startEndStep = high_resolution_clock::now();
        bpWriter.EndStep();
        stopEndStep = high_resolution_clock::now();
    }

    // durationPut = duration_cast<microseconds>(stopPut - startPut);
    // durationEndStep = duration_cast<microseconds>(stopEndStep -
    // startEndStep);
    durationWrite = duration_cast<microseconds>(stopEndStep - startPut);

    // std::ofstream timeOutput;
    // std::ofstream timeOutput("heatTransfer-Output.txt");
    // timeOutput.open("heatTransfer-Output.txt", std::fstream::app);
    // if (timeOutput.is_open())
    // {
    // timeOutput << "\n--- Write time in mikroseconds ---\n" << std::endl;
    // timeOutput << "put: \t rank: \t" << s.rank << "\t" << durationPut.count()
    // << std::endl;
    // timeOutput << "step: \t rank: \t" << s.rank << "\t" <<
    // durationEndStep.count()
    // << std::endl;
    // timeOutput << "write: \t rank: \t" << s.rank << "\t"
    // << durationWrite.count()
    // << "\n"
    // << std::endl;
    // timeOutput.close();

    // std::stringstream outputBuffer;
    // outputBuffer << "put: \t rank: \t" << s.rank << "\t" <<
    // durationPut.count()
    //              << "\n"
    //              << "step: \t rank: \t" << s.rank << "\t"
    //              << durationEndStep.count() << "\n"
    //              << "write: \t rank: \t" << s.rank << "\t"
    //              << durationWrite.count();
    //              // << durationWrite.count() << std::endl;

    // std::string output = outputBuffer.str();
    // std::string input = std::string(output);

    // std::cout << output << std::endl;

    size_t writeSum = 0;
    size_t writeMean = 0;
    size_t write = durationWrite.count();

    MPI_Reduce(&write, &writeSum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    writeMean = writeSum / s.nproc;

    if (s.rank == 0)
    {
        // std::cout << outputBuffer.str() << std::endl;
        // std::cout << "put: \t rank: \t" << s.rank << "\t" <<
        // durationPut.count()
        //           << "\n"
        //           << "step: \t rank: \t" << s.rank << "\t"
        //           << durationEndStep.count() << "\n"
        //           << "write: \t rank: \t" << s.rank << "\t"
        //           << durationWrite.count() << std::endl;
        // writeMean;
        std::cout << writeMean;
        std::cout << "\t " << write;
        for (int i = 1; i < s.nproc; i++)
        {
            // size_t put, step, write;
            MPI_Status status;

            // MPI_Recv(&put, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &status);
            // MPI_Recv(&step, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &status);
            MPI_Recv(&write, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &status);
            // std::cout << "put: \t rank: \t" << i << "\t" << put << "\n"
            //           << "step: \t rank: \t" << i << "\t" << step << "\n"
            //           << "write: \t rank: \t" << i << "\t" << write
            //           << std::endl;

            std::cout << "\t " << write;
        }
        std::cout << std::endl;
    }
    else
    {
        // size_t put = durationPut.count();
        // size_t step = durationEndStep.count();
        // size_t write = durationWrite.count();

        // MPI_Send(&put, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
        // MPI_Send(&step, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
        MPI_Send(&write, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
    }

    // std::cout << "put: \t rank: \t" << s.rank << "\t" << durationPut.count()
    //           << "\n"
    //         << "step: \t rank: \t" << s.rank << "\t" <<
    //         durationEndStep.count()
    //           << "\n"
    // << "write: \t rank: \t" << s.rank << "\t" << durationWrite.count()
    //           << std::endl;
    // }
}

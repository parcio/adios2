/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Write a global array from multiple processors.
 *
 * A global array is an N-dimensional array. A process can write a sub-array
 * into the global array by stating the N-dimensional offset and the size of
 * the sub-array. At reading, one can read back any portion of the array
 * regardless of how many processors wrote that data.
 *
 * Processes are NOT required
 * - to stay in the boundaries of the global dimensions. However, one will not
 * be able to read back data outside of the boundaries.
 * - to fill the whole global array, i.e. one can leave holes in it. At reading,
 * one will get the fill-value set for the array for those coordinates that
 * are not written by any process.
 *
 * The global dimensions of a global array MUST NOT change over time.
 * If they are, then the array should be handled as a local array. Of course, if
 * only a single output step is written to a file, that still shows up at
 * reading as a global array.
 *
 * The decomposition of the array across the processes, however, can change
 * between output steps.
 *
 * Created on: Jun 2, 2017
 *      Author: pnorbert
 */

#include <iostream>
#include <vector>
#include <unistd.h>

#include <adios2.h>
#if ADIOS2_USE_MPI
#include <mpi.h>
#endif

int main(int argc, char *argv[])
{
    int rank = 0, nproc = 1;
#if ADIOS2_USE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
#endif
    const int NSTEPS = 5;

#if ADIOS2_USE_MPI
    adios2::ADIOS adios(MPI_COMM_WORLD);
#else
    adios2::ADIOS adios;
#endif

    // Application variables for output
    const unsigned int Nx = 10;
    // Global 2D array, size of nproc x Nx, with 1D decomposition
    // Each process writes one "row" of the 2D matrix.
    std::vector<double> row(Nx);
    std::vector<double> row2(Nx);

    try
    {
        // Get io settings from the config file or
        // create one with default settings here
        adios2::IO io = adios.DeclareIO("Output");
        io.SetEngine("julea-db-dai");
        // io.SetEngine("julea-db");
        // io.SetEngine("BP4");

        /*
         * Define global array: type, name, global dimensions
         * The local process' part (start, count) can be defined now or later
         * before Write().
         */
        adios2::Variable<double> varGlobalArray =
            io.DefineVariable<double>("Variable1", {(unsigned int)nproc, Nx});

        // Open file. "w" means we overwrite any existing file on disk,
        // but Advance() will append steps to the same file.
        adios2::Engine writer = io.Open("globalArray.jb", adios2::Mode::Write);
        // adios2::Engine writer = io.Open("globalArray.bp", adios2::Mode::Write);

        // std::cout << "rank = " << rank << std::endl;
        // std::cout << "nproc = " << nproc << std::endl;
        for (size_t step = 0; step < NSTEPS; step++)
        {
            writer.BeginStep();

            for (size_t i = 0; i < Nx; i++)
            {
                row2[i] = step * Nx * nproc * 1.0 + rank * Nx * 1.0 + (double)i;
                // std::cout << "row2: " << row2[i] << std::endl;
            }

            // Make a 2D selection to describe the local dimensions of the
            // variable we write and its offsets in the global spaces
            // adios2::SelectionBoundingBox sel();
            varGlobalArray.SetSelection(adios2::Box<adios2::Dims>(
                {static_cast<size_t>(rank), 0}, {1, static_cast<size_t>(Nx)}));
            // writer.Put<double>(varGlobalArray, row.data(),adios2::Mode::Sync);

            if (rank == 0)
            {
                sleep(10);
                row2[0] = 42;
                // writer.Put<double>(varGlobalArray, row2.data(), adios2::Mode::Sync);
                writer.Put<double>(varGlobalArray, row2.data(), adios2::Mode::Deferred);
            }

            if (rank == 1)
            {
                row2[0] = 1337;
                sleep(10);
                writer.Put<double>(varGlobalArray, row2.data(),adios2::Mode::Deferred);
            }
            if (rank == 2)
            {
                row2[0] = 666;
                sleep(10);
                writer.Put<double>(varGlobalArray, row2.data(),adios2::Mode::Deferred);
            }
            if (rank == 3)
            {
                row2[0] = 424242;
                sleep(10);
                writer.Put<double>(varGlobalArray, row2.data(),adios2::Mode::Deferred);
            }

            // Indicate we are done for this step.
            // Disk I/O will be performed during this call unless
            // time aggregation postpones all of that to some later step
            writer.EndStep();
        }

        // Called once: indicate that we are done with this output for the run
        writer.Close();
    }
    catch (std::invalid_argument &e)
    {
        if (rank == 0)
        {
            std::cout << "Invalid argument exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::ios_base::failure &e)
    {
        if (rank == 0)
        {
            std::cout << "System exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::exception &e)
    {
        if (rank == 0)
        {
            std::cout << "Exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }

#if ADIOS2_USE_MPI
    MPI_Finalize();
#endif

    return 0;
}

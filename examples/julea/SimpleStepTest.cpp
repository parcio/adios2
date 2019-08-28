/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Write local arrays from multiple processors.
 *
 * If one cannot or does not want to organize arrays present on each process
 * as one global array, still one can write them out with the same name.
 * Reading, however, needs to be handled differently: each process' array has
 * to be read separately, using Writeblock selections. The size of each process
 * block should be discovered by the reading application by inquiring per-block
 * size information of the variable, and allocate memory for reading
 * accordingly.
 *
 * In this example we write v0, v1, v2 and v3, in 5 output steps, where
 * v0 has the same size on every process at every step
 * v1 has different size on each process but fixed over time
 * v2 has different size on each process and that is changing over time
 * v3 is like v2 but also the number of processes writing it changes over time
 *
 * bpls can show the size of each block of the variable:
 *
 * $ cd <adios build directory>
 * $ make
 * $ mpirun -n 4 ./bin/localArray
 * $ bpls -l localArray.bp
 * double   v0    5*[4]*{6} = 0 / 3.4
 * double   v1    5*[4]*{__} = 0 / 3.4
 * double   v2    5*[4]*{__} = 0 / 3.4
 * double   v3    5*[__]*{__} = 0 / 3.3
 *
 * Study the decomposition of each variable
 * $ bpls -l localArray.bp -D v0
 * and notice the progression in the changes.
 *
 * Created on: Jun 2, 2017
 *      Author: pnorbert
 */

#include <iostream>
#include <vector>

#include <adios2.h>
#ifdef ADIOS2_HAVE_MPI
#include <mpi.h>
#endif

int main(int argc, char *argv[])
{
    int rank = 0;

    // v0 has the same size on every process at every step
    const size_t Nglobal = 6;
    std::vector<double> v0(Nglobal);

    try
    {
        adios2::IO io = adios.DeclareIO("Output");
        // io.SetEngine("julea-kv");
        /*
         * Define local array: type, name, local size
         * Global dimension and starting offset must be an empty vector
         * Here the size of the local array is the same on every process
         */
        adios2::Variable<double> varV0 =
            io.DefineVariable<double>("v0", {}, {}, {Nglobal});

        // Open file. "w" means we overwrite any existing file on disk,
        // but Advance() will append steps to the same file.
        adios2::Engine writer = io.Open("JULEAlocalArray.bp", adios2::Mode::Write);

        for (int step = 0; step < 5; step++)
        {
            writer.BeginStep();

            // v0
            for (size_t i = 0; i < Nglobal; i++)
            {
                v0[i] = rank * 1.0 + step * 0.1;
            }
            writer.Put<double>(varV0, v0.data());

            writer.EndStep();
        }

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

#ifdef ADIOS2_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}

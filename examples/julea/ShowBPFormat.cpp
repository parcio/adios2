#include <iostream> //std::cout
#include <vector>

#include <adios2.h>


void write_simple(std::string engine, std::string fileName)
{
    const size_t Nglobal = 2;
    // std::vector<double> v0(Nglobal);
    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);
    std::vector<double> v5(Nglobal);
    std::vector<double> v6(Nglobal);
    std::vector<double> v7(Nglobal);
    std::vector<double> v8(Nglobal);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {}, {}, {Nglobal});

    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);


    for (size_t i = 0; i < Nglobal; i++)
    {
        // v1[i] = 11 * (step + 1) + step * 0.1 + i * 100;
        // v2[i] = 22 * (step + 1) + step * 0.1 + i * 100;
        // v3[i] = 33 * (step + 1) + step * 0.1 + i * 100;
        // v4[i] = 44 * (step + 1) + step * 0.1 + i * 100;
        // v5[i] = 55 * (step + 1) + step * 0.1 + i * 100;
        // v6[i] = 66 * (step + 1) + step * 0.1 + i * 100;
        // v7[i] = 77 * (step + 1) + step * 0.1 + i * 100;
        // v8[i] = 88 * (step + 1) + step * 0.1 + i * 100;
        v1[i] = 11 + 1 * 0.1 + i * 100;
        v2[i] = 22 + 2 * 0.1 + i * 100;
        v3[i] = 33 + 3 * 0.1 + i * 100;
        v4[i] = 44 + 4 * 0.1 + i * 100;
        v5[i] = 55 + 5 * 0.1 + i * 100;
        v6[i] = 66 + 6 * 0.1 + i * 100;
        v7[i] = 77 + 7 * 0.1 + i * 100;
        v8[i] = 88 + 8 * 0.1 + i * 100;


        std::cout << "v1[" << i << "]: " << v1[i] << std::endl;
        std::cout << "v2[" << i << "]: " << v2[i] << std::endl;
        std::cout << "v3[" << i << "]: " << v3[i] << std::endl;
        std::cout << "v4[" << i << "]: " << v4[i] << std::endl;
        std::cout << "v5[" << i << "]: " << v5[i] << std::endl;
        std::cout << "v6[" << i << "]: " << v6[i] << std::endl;
        std::cout << "v7[" << i << "]: " << v7[i] << std::endl;
        std::cout << "v8[" << i << "]: " << v8[i] << std::endl;
    }


    writer.Put<double>(varV0, v1.data(), adios2::Mode::Deferred);
    writer.Put<double>(varV0, v2.data(), adios2::Mode::Sync);
    writer.Put<double>(varV0, v3.data(), adios2::Mode::Deferred);

    for (int step = 0; step < 3; step++)
    {

        writer.BeginStep();

        writer.Put<double>(varV0, v4.data(), adios2::Mode::Sync);
    	writer.Put<double>(varV0, v5.data(), adios2::Mode::Deferred);
        writer.Put<double>(varV0, v6.data(), adios2::Mode::Sync);
    	writer.Put<double>(varV0, v7.data(), adios2::Mode::Deferred);

        if (step == 2)
        {
        	writer.Put<double>(varV0, v8.data(), adios2::Mode::Sync);
        }

        std::cout << "\n---------- Application: EndStep "
                     "-------------------------------------\n"
                  << std::endl;
        writer.EndStep();
    }

    std::cout << "\n---------- Application: left for loop "
                 "-------------------------------------\n"
              << std::endl;
    // io.FlushAll();
    writer.Close();
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... SimpleStepTest ... " << std::endl;
    std::cout << "... Only one process ... " << std::endl;

    try
    {

        write_simple("bp3", "SimpleSteps.bp");
        // write_simple("bp4", "SimpleSteps.bp");
        // write_simple("julea-kv", "SimpleSteps.jv");
        // write_simple("julea-db", "SimpleSteps.jb");

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

    // #ifdef ADIOS2_HAVE_MPI
    //     MPI_Finalize();
    // #endif

    return 0;
}

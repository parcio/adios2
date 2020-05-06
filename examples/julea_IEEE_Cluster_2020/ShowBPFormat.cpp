#include <iostream>
#include <vector>

#include <adios2.h>

void printVector(std::string name, std::vector<double> variable, size_t nglobal)
{
    std::cout << "-- variable: " << name << std::endl;
    for (size_t i = 0; i < nglobal; i++)
    {
        std::cout << "v[" << i << "] = " << variable[i] << std::endl;
    }
}

void writeSimple(std::string engine, std::string fileName,
                 bool printVectorContent, size_t vectorSize, size_t numberSteps)
{
    const size_t Nglobal = vectorSize;

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
        v1[i] = 11 + 1 * 0.1 + i * 100;
        v2[i] = 22 + 2 * 0.1 + i * 100;
        v3[i] = 33 + 3 * 0.1 + i * 100;
        v4[i] = 44 + 4 * 0.1 + i * 100;
        v5[i] = 55 + 5 * 0.1 + i * 100;
        v6[i] = 66 + 6 * 0.1 + i * 100;
        v7[i] = 77 + 7 * 0.1 + i * 100;
        v8[i] = 88 + 8 * 0.1 + i * 100;
    }

    if (printVectorContent)
    {
        printVector("vector_1", v1, Nglobal);
        printVector("vector_2", v2, Nglobal);
        printVector("vector_3", v3, Nglobal);
        printVector("vector_4", v4, Nglobal);
        printVector("vector_5", v5, Nglobal);
        printVector("vector_6", v6, Nglobal);
        printVector("vector_7", v7, Nglobal);
        printVector("vector_8", v8, Nglobal);
    }

    writer.Put<double>(varV0, v1.data(), adios2::Mode::Deferred);
    writer.Put<double>(varV0, v2.data(), adios2::Mode::Sync);
    writer.Put<double>(varV0, v3.data(), adios2::Mode::Deferred);

    for (int i = 0; i < numberSteps; i++)
    {
        writer.BeginStep();

        writer.Put<double>(varV0, v4.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v5.data(), adios2::Mode::Deferred);
        writer.Put<double>(varV0, v6.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v7.data(), adios2::Mode::Deferred);

        if (i == 2)
        {
            writer.Put<double>(varV0, v8.data(), adios2::Mode::Sync);
        }

        writer.EndStep();
    }
    writer.Close();
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "\n____ A programm to write a simple bp file with steps and "
                 "blocks ____ "
              << std::endl;
    std::cout << "... execute one of the following commands in your shell to "
                 "inspect the file with bpls "
              << std::endl;
    std::cout << "\n... 'bpls -D SimpleSteps.bp' to show variable decomposition"
              << std::endl;
    std::cout << "... 'bpls -d SimpleSteps.bp' to dump variable content"
              << std::endl;
    std::cout << "... 'bpls -d -l SimpleSteps.bp' to dump variable content "
                 "with min/max values\n"
              << std::endl;

    try
    {
        // engine, fileName, printOutput, vectorSize, numberSteps
        writeSimple("bp3", "SimpleSteps.bp", true, 4, 3);
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
    return 0;
}

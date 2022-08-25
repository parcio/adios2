void JuleaKVInteractionReader::DefineVariableInInit(
    core::IO *io, const std::string varName, std::string stringType, Dims shape,
    Dims start, Dims count, bool constantDims, bool isLocalValue)
{
    const char *type = stringType.c_str();

    if (strcmp(type, "unknown") == 0)
    {
        // TODO
    }
    else if (strcmp(type, "compound") == 0)
    {
    }
    else if (strcmp(type, "string") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::string>(varName, shape, start,
                                                        count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::string>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "int8_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int8_t>(varName, shape, start, count,
                                                   constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int8_t>(varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "uint8_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint8_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint8_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int16_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int16_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int16_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint16_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint16_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint16_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int32_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int32_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int32_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint32_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint32_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint32_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int64_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int64_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int64_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint64_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint64_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint64_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "float") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<float>(varName, shape, start, count,
                                                  constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<float>(varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<double>(varName, shape, start, count,
                                                   constantDims);
        }
        else
        {
            // std::cout << "Single Value double " << std::endl;
            auto &var = io->DefineVariable<double>(varName, {1}, {0}, {1});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "long double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<long double>(varName, shape, start,
                                                        count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<long double>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex float") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::complex<float>>(
                varName, shape, start, count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::complex<float>>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::complex<double>>(
                varName, shape, start, count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::complex<double>>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }

    // std::map<std::string, Params> varMap = io->GetAvailableVariables();

    // for (std::map<std::string, Params>::iterator it = varMap.begin();
    //      it != varMap.end(); ++it)
    // {

    //     // std::cout << "first: " << it->first << " => " <<
    //     it->second.begin()
    //     // << '\n';
    //     // std::cout << "first: " << it->first << '\n';
    // }
}
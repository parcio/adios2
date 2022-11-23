/*
 * ADIOS is freely available under the terms of the BSD license described
 * in the COPYING file in the top level directory of this source distribution.
 *
 *  Created on: November 2022
 *      Author: Kira Duwe
 */

/*
 * Import NetCDF files (.nc) into ADIOS2. 
 * Depending on the used engine, the data will be stored in any of the BP formats, HDF5 or inside of JULEA. 
 * In JULEA the metadata can be stored in a key-value store or a database, while the data is stored in an object store.
 *
 **/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "import_nc.h"
// #include "verinfo.h"

// #include <chrono>
// #include <cinttypes>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include <errno.h>



// #include "adios2/helper/adiosString.h" // EndsWith
// #include "adios2/helper/adiosSystem.h" //isHDF5File
// #include <adios2sys/CommandLineArguments.hxx>
// #include <adios2sys/SystemTools.hxx>
// #include <pugixml.hpp>

namespace adios2
{
namespace utils
{

using EntryMap = std::map<std::string, Entry>;

// global variables
// Values from the arguments or defaults

// output files' starting path (can be extended with subdirs,
// names, indexes)
std::string outpath;
char *varmask[MAX_MASKS]; // can have many -var masks (either shell patterns or
                          // extended regular expressions)
int nmasks;               // number of masks specified
char *vfile;              // file name to bpls
std::string start;        // dimension spec starting points
std::string count;        // dimension spec counts
std::string format;       // format string for one data element (e.g. %6.2f)

// Flags from arguments or defaults
bool dump; // dump data not just list info(flag == 1)
bool output_xml;
bool use_regexp;       // use varmasks as regular expressions
bool sortnames;        // sort names before listing
bool listattrs;        // do list attributes too
bool listmeshes;       // do list meshes too
bool attrsonly;        // do list attributes only
bool longopt;          // -l is turned on
bool timestep;         // read step by step
bool noindex;          // do no print array indices with data
bool printByteAsChar;  // print 8 bit integer arrays as string
bool plot;             // dump histogram related information
bool hidden_attrs;     // show hidden attrs in BP file
int hidden_attrs_flag; // to be passed on in option struct
bool show_decomp;      // show decomposition of arrays
bool show_version;     // print binary version info of file before work

// other global variables
char *prgname; /* argv[0] */
// long timefrom, timeto;
int64_t istart[MAX_DIMS], icount[MAX_DIMS]; // negative values are allowed
int ndimsspecified = 0;
#ifdef USE_C_REGEX
regex_t varregex[MAX_MASKS]; // compiled regular expressions of varmask
#else
std::vector<std::regex> varregex;
#endif
int ncols = 6; // how many values to print in one row (only for -p)
int verbose = 0;
FILE *outf; // file to print to or stdout
char commentchar;

// help function
void display_help()
{
    // printf( "Usage: %s  \n", prgname);
    printf(
        "usage: import_nc input_file output_file engine\n"
        "\nImport a NetCDF file (.nc) into ADIOS2. \n"
        "The given file will be imported using the specified engine "
        "and stored in the corresponding format using the passed \n"
        "output file name. \n"
        "Note that using the JULEA engines the data is stored only \n"
        "in JULEA. No separate file is created in contrast to using \n"
        "the ADIOS2 engines. \n"
        "\n"
        "  --long      | -l           Print values of all scalars and "
        "\n"
        "Help options\n"
        "  --help      | -h           Print this help.\n"
        "  --verbose   | -v           Print log about what this program is "
        "doing.\n"
        // "                               Use multiple -v to increase logging "
        // "level.\n"
        // "  --version   | -V           Print version information; compatible "
        // " with\n"
        // "                               --verbose for additional information, "
        // "i.e.\n"
        // "                               -v --version.\n"
        "\nTypical use: import_nc -lav <input_file> <output_file> <engine>\n");
        "\nExample: import_nc weather.nc weather.bp bp4\n");
        "\nExample: import_nc weather.nc weather.bp julea-db\n");
}

bool option_help_was_called = false;
int optioncb_help(const char *argument, const char *value, void *call_data)
{
    // adios2sys::CommandLineArguments *arg =
    // static_cast<adios2sys::CommandLineArguments *>(call_data);
    // printf("%s\n", arg->GetHelp());
    display_help();
    option_help_was_called = true;
    return 1;
}

/** Main */
int import_ncMain(int argc, char *argv[])
{
    int retval = 0;

    init_globals();

    adios2sys::CommandLineArguments arg;
    arg.Initialize(argc, argv);
    typedef adios2sys::CommandLineArguments argT;
    arg.StoreUnusedArguments(true);
    arg.AddCallback("-v", argT::NO_ARGUMENT, optioncb_verbose, nullptr, "");
    arg.AddCallback("--verbose", argT::NO_ARGUMENT, optioncb_verbose, nullptr,
                    "Print information about what bpls is doing");
    arg.AddCallback("--help", argT::NO_ARGUMENT, optioncb_help, &arg, "Help");
    arg.AddCallback("-h", argT::NO_ARGUMENT, optioncb_help, &arg, "");
    arg.AddBooleanArgument("--dump", &dump,
                           "Dump matched variables/attributes");
    arg.AddBooleanArgument("-d", &dump, "");
    arg.AddBooleanArgument(
        "--long", &longopt,
        "Print values of all scalars and attributes and min/max "
        "values of arrays");
    arg.AddBooleanArgument("-l", &longopt, "");
    arg.AddBooleanArgument("--regexp", &use_regexp,
                           "| -e Treat masks as extended regular expressions");
    arg.AddBooleanArgument("-e", &use_regexp, "");
    arg.AddArgument("--output", argT::SPACE_ARGUMENT, &outpath,
                    "| -o opt    Print to a file instead of stdout");
    arg.AddArgument("-o", argT::SPACE_ARGUMENT, &outpath, "");
    arg.AddArgument("--start", argT::SPACE_ARGUMENT, &start,
                    "| -s opt    Offset indices in each dimension (default is "
                    "0 for all dimensions).  opt<0 is handled as in python (-1 "
                    "is last)");
    arg.AddArgument("-s", argT::SPACE_ARGUMENT, &start, "");
    arg.AddArgument("--count", argT::SPACE_ARGUMENT, &count,
                    "| -c opt    Number of elements in each dimension. -1 "
                    "denotes 'until end' of dimension. default is -1 for all "
                    "dimensions");
    arg.AddArgument("-c", argT::SPACE_ARGUMENT, &count, "");
    arg.AddBooleanArgument("--noindex", &noindex,
                           " | -y Print data without array indices");
    arg.AddBooleanArgument("-y", &noindex, "");
    arg.AddBooleanArgument("--timestep", &timestep,
                           " | -t Print values of timestep elements");
    arg.AddBooleanArgument("-t", &timestep, "");
    arg.AddBooleanArgument("--attrs", &listattrs,
                           " | -a List/match attributes too");
    arg.AddBooleanArgument("-a", &listattrs, "");
    arg.AddBooleanArgument("--attrsonly", &attrsonly,
                           " | -A List/match attributes only (no variables)");
    arg.AddBooleanArgument("-A", &attrsonly, "");
    arg.AddBooleanArgument("--meshes", &listmeshes, " | -m List meshes");
    arg.AddBooleanArgument("-m", &listmeshes, "");
    arg.AddBooleanArgument("--string", &printByteAsChar,
                           " | -S Print 8bit integer arrays as strings");
    arg.AddBooleanArgument("-S", &printByteAsChar, "");
    arg.AddArgument("--columns", argT::SPACE_ARGUMENT, &ncols,
                    "| -n opt    Number of data elements per row to print");
    arg.AddArgument("-n", argT::SPACE_ARGUMENT, &ncols, "");
    arg.AddArgument("--format", argT::SPACE_ARGUMENT, &format,
                    "| -f opt    Format string to use for one data item ");
    arg.AddArgument("-f", argT::SPACE_ARGUMENT, &format, "");
    arg.AddBooleanArgument("--hidden_attrs", &hidden_attrs,
                           "  Show hidden ADIOS attributes in the file");
    arg.AddBooleanArgument(
        "--decompose", &show_decomp,
        "| -D Show decomposition of variables as layed out in file");
    arg.AddBooleanArgument("-D", &show_decomp, "");
    arg.AddBooleanArgument(
        "--version", &show_version,
        "Print version information (add -verbose for additional"
        " information)");
    arg.AddBooleanArgument("-V", &show_version, "");

    if (!arg.Parse())
    {
        fprintf(stderr, "Parsing arguments failed\n");
        return 1;
    }
    if (option_help_was_called)
        return 0;

    retval = process_unused_args(arg);
    if (retval)
    {
        return retval;
    }
    if (option_help_was_called)
        return 0;

    return retval;
}

void init_globals()
{
    int i;
    verbose = 0;
}


int main(int argc, char *argv[])
{
    int retval = 1;
    try
    {
        retval = adios2::utils::import_ncMain(argc, argv);
    }
    catch (std::exception &e)
    {
        std::cout << "\nipmport_nc caught an exception\n";
        std::cout << e.what() << std::endl;
    }
    return retval;
}

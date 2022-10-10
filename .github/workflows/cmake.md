name: Dependencies
on:
  schedule:
    # TODO figure out which will work best here
    # Is there possibility to note updates in JULEA fork?
    
jobs:
  dependencies:
    name: JULEA Dependency
    runs-on: # use matrix here?
    # timeout?
    strategy:
      fail-fast: false
      matrix:
        os:
          # TODO
          
    steps:
    # TODO: run for official JULEA as well in case local fork does not have all updates?
      - name: Checkout JULEA
      # TODO: with JULEA system packages have to be removed; necessary here as well?
        run: |
          # TODO: only DAI branch for now
          git clone https://github.com/Bella42/julea.git
      - name: Install JULEA dependencies
      - name: Set up MySQL
        if: ${{ matrix.julea.db == 'mysql' && matrix.julea.db-server == 'mysql' }}
      - name: Set up MariaDB
        if: ${{ matrix.julea.db == 'mysql' && matrix.julea.db-server == 'mariadb' }}
      - name: Setup JULEA environment
        env:
        run: |
          . scripts/environment.sh
      - name: Configure JULEA
      - name: Compile JULEA
        run: ninja -C bld
      - name: Create JULEA configuration
        run: |
          . scripts/environment.sh
          # TODO: set JULEA components
      - name: Run JULEA tests
        env:
            LSAN_OPTIONS: exitcode=0
        run: |
          . scripts/environment.sh
          ./scripts/setup.sh start
          ./scripts/test.sh
          sleep 10
          ./scripts/test.sh
          ./scripts/setup.sh stop
    - name: Make example
    - name: Run example
    


name: CI

on:
  push:
    branches: [ "master" ]
  pull_request:
    branches: [ "master" ]
  # TODO: when JULEA is updated

env:
  # TODO: for Debug build as well?
  BUILD_TYPE: Release

jobs:
  build:
    # TODO: build for different os?
    runs-on: ubuntu-latest

    steps:
    # TODO: update this example line
    - uses: actions/checkout@v3

    - name: Configure CMake
      # TODO: turn JULEA on when present
      run: cmake -B ${{github.workspace}}/build -DCMAKE_BUILD_TYPE=${{env.BUILD_TYPE}} -DADIOS2_USE_Fortran=OFF -DADIOS2_USE_MPI=ON -DADIOS2_USE_DataMan=OFF -DADIOS2_USE_BZip2=OFF -DADIOS2_USE_PNG=OFF -DADIOS2_USE_JULEA=OFF

    - name: Build
      # TODO: what number of processes?
      run: make -j

    - name: Test
      working-directory: ${{github.workspace}}/build
      # Execute tests defined by the CMake configuration.  
      # See https://cmake.org/cmake/help/latest/manual/ctest.1.html for more detail
      run: ctest -C ${{env.BUILD_TYPE}}
      

# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/baymax/alidb/lindorm-tsdb-contest-cpp

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/baymax/alidb/lindorm-tsdb-contest-cpp/build

# Include any dependencies generated for this target.
include CMakeFiles/perfTest.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/perfTest.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/perfTest.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/perfTest.dir/flags.make

CMakeFiles/perfTest.dir/perfTest.cpp.o: CMakeFiles/perfTest.dir/flags.make
CMakeFiles/perfTest.dir/perfTest.cpp.o: ../perfTest.cpp
CMakeFiles/perfTest.dir/perfTest.cpp.o: CMakeFiles/perfTest.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/baymax/alidb/lindorm-tsdb-contest-cpp/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/perfTest.dir/perfTest.cpp.o"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/perfTest.dir/perfTest.cpp.o -MF CMakeFiles/perfTest.dir/perfTest.cpp.o.d -o CMakeFiles/perfTest.dir/perfTest.cpp.o -c /home/baymax/alidb/lindorm-tsdb-contest-cpp/perfTest.cpp

CMakeFiles/perfTest.dir/perfTest.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/perfTest.dir/perfTest.cpp.i"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/baymax/alidb/lindorm-tsdb-contest-cpp/perfTest.cpp > CMakeFiles/perfTest.dir/perfTest.cpp.i

CMakeFiles/perfTest.dir/perfTest.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/perfTest.dir/perfTest.cpp.s"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/baymax/alidb/lindorm-tsdb-contest-cpp/perfTest.cpp -o CMakeFiles/perfTest.dir/perfTest.cpp.s

# Object files for target perfTest
perfTest_OBJECTS = \
"CMakeFiles/perfTest.dir/perfTest.cpp.o"

# External object files for target perfTest
perfTest_EXTERNAL_OBJECTS =

perfTest: CMakeFiles/perfTest.dir/perfTest.cpp.o
perfTest: CMakeFiles/perfTest.dir/build.make
perfTest: libyourDbLib.a
perfTest: CMakeFiles/perfTest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/baymax/alidb/lindorm-tsdb-contest-cpp/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable perfTest"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/perfTest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/perfTest.dir/build: perfTest
.PHONY : CMakeFiles/perfTest.dir/build

CMakeFiles/perfTest.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/perfTest.dir/cmake_clean.cmake
.PHONY : CMakeFiles/perfTest.dir/clean

CMakeFiles/perfTest.dir/depend:
	cd /home/baymax/alidb/lindorm-tsdb-contest-cpp/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/baymax/alidb/lindorm-tsdb-contest-cpp /home/baymax/alidb/lindorm-tsdb-contest-cpp /home/baymax/alidb/lindorm-tsdb-contest-cpp/build /home/baymax/alidb/lindorm-tsdb-contest-cpp/build /home/baymax/alidb/lindorm-tsdb-contest-cpp/build/CMakeFiles/perfTest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/perfTest.dir/depend


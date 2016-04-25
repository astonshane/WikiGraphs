#include <stdio.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>

typedef std::map<std::string, std::set<std::string>> LinkMap;

int g_world_size;
int g_mpi_rank;
unsigned int g_total_files = 125;
unsigned int g_file;
unsigned int g_user_start;
unsigned int g_num_users;

void set_positions();

void parse_file();


int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(&argc, &argv);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);


  // Print off a hello world message
  //printf("Hello world from rank %d\n", g_mpi_rank);
  set_positions();



  // Open up the wikipedia xml file
  //MPI_File infile;
  //MPI_File_open(MPI_COMM_WORLD, (char *)g_filename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);

  // Parse the file
  if (g_mpi_rank == 0){
    parse_file();
  }
  // Close the file
  //MPI_File_close(&infile);

  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void set_positions(){
  int count = 0;
  bool found = false;
  for (int f=0; f<g_total_files; f++) {
    int ranks_per_file = g_world_size / g_total_files;
    int extra = g_world_size % g_total_files;
    if (f < extra){
      ranks_per_file += 1;
    }
    for (int i=0; i<ranks_per_file; i++){
      if (count == g_mpi_rank){
        g_file = f;
        g_user_start = (1000000/ranks_per_file)*i;
        g_num_users = 1000000/ranks_per_file;
        if (i == ranks_per_file-1){
          g_num_users += 1000000 - g_num_users*(i+1);
        }
        found = true;
        break;
      }else{
        count += 1;
      }
    }
    if (found){
      break;
    }
  }
  printf("Rank %d:      g_file: %d       g_user_start: %d     g_num_users: %d\n", g_mpi_rank, g_file, g_user_start, g_num_users);
}

// Parse the input file: pull out page titles and any links from a page to another
void parse_file(){
  printf("parsing the file...\n");

}

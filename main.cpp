#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>

int g_world_size;
int g_mpi_rank;
int g_global_max_id = 125000000;
int g_local_max_id;
std::map<int, std::set<int>> g_adj_list;

void parse_file();
void add_to_adjlist(std::string line);

int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(&argc, &argv);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);


  // Print off a hello world message
  //parse_file();

  if (g_mpi_rank == 1){
    parse_file();
    printf("Rank: %d    finished parse_file()\n", g_mpi_rank);
    printf("Rank: %d    g_adj_list.size(): %lu\n", g_mpi_rank, g_adj_list.size());
  }



  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void parse_file(){
  printf("parsing the file...\n");
  g_local_max_id = ceil((float)g_global_max_id / g_world_size)*(g_mpi_rank+1);
  printf("maximum id = %d\n", g_local_max_id);
  char filename[100];

  sprintf(filename, "com-friendster.ungraph.txt");
  //sprintf(filename, "/gpfs/u/home/PCP5/PCP5stns/scratch-shared/friendster_data/friends-%s%d______.txt", padding.c_str(), g_file);
  //printf("rank: %d  %s\n", g_mpi_rank, filename);

  MPI_File infile;
  MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  MPI_Offset filesize = 0;
  MPI_File_get_size(infile, &filesize);

  //printf("rank %d: %lld\n", g_mpi_rank, filesize);
  printf("rank %d: opened the file! %lld\n", g_mpi_rank, filesize);

  char * buffer;
  std::string chunk = "";
  unsigned int buffer_size = 1000;
  MPI_Offset offset = 124; // get rid of the stuff at the top of the file

  while(offset < filesize){
   buffer = (char *)malloc( (buffer_size + 1)*sizeof(char));
   // read it in
   MPI_File_read_at(infile, offset, buffer, buffer_size, MPI_CHAR, MPI_STATUS_IGNORE);
   offset += buffer_size;
   // end the string
   buffer[buffer_size] = '\0';
   std::string new_string(buffer);
   free(buffer);
   chunk += new_string;
   //std::cout << chunk << std::endl;;

   int found_newline = chunk.find('\n');


   while (found_newline < chunk.size()){
    std::string line = chunk.substr(0, found_newline);
    add_to_adjlist(line);
    chunk = chunk.substr(found_newline+1);
    found_newline = chunk.find('\n');
  }
 }

  MPI_File_close(&infile);
}

void add_to_adjlist(std::string line){
  //std::cout << "adding: " << line << " to adj list" << std::endl;
  int tab_pos = line.find("\t");
  int one = atoi(line.substr(0, tab_pos).c_str());
  int two = atoi(line.substr(tab_pos+1).c_str());
  if (one < g_local_max_id){
    g_adj_list[one].insert(two);
    //printf("(ONE) Adding %d -> %d\n", one, two);
  }else if (two < g_local_max_id){
    g_adj_list[two].insert(one);
    //sprintf("(TWO) Adding %d -> %d\n", two, one);
  }
}

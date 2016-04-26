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
std::map<int, std::set<int>> g_adj_list;

int global_count = 0;

void set_positions();

void parse_file();

void find_friends(int id, std::string chunk);


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
  parse_file();
  if (g_mpi_rank == 0){
    //parse_file();
    //printf("Rank %d: global_count: %d\n", g_mpi_rank, global_count);
    //printf("final size of adj_list: %lu\n", g_adj_list.size());
  }
  printf("Rank %d: global_count: %d\n", g_mpi_rank, global_count);
  printf("final size of adj_list: %lu\n", g_adj_list.size());

  // Close the file
  //MPI_File_close(&infile);

  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void parse_file(){
  printf("parsing the file...\n");
  char filename[100];
  std::string padding;
  if (g_file < 10){
    padding = "00";
  }else if (g_file < 100) {
    padding = "0";
  } else {
    padding = "";
  }

  sprintf(filename, "data/friends-%s%d______.txt", padding.c_str(), g_file);
  //sprintf(filename, "/gpfs/u/home/PCP5/PCP5stns/scratch-shared/friendster_data/friends-%s%d______.txt", padding.c_str(), g_file);
  printf("rank: %d  %s\n", g_mpi_rank, filename);

  MPI_File infile;
  MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  MPI_Offset filesize = 0;
  MPI_File_get_size(infile, &filesize);

  //printf("rank %d: %lld\n", g_mpi_rank, filesize);
  printf("rank %d: opened the file! %lld\n", g_mpi_rank, filesize);

  int starting_id = 1000000*g_file + g_user_start;
  int final_id = starting_id + g_num_users;

  printf("rank %d: starting_id: %d final_id: %d\n", g_mpi_rank, starting_id, final_id);

  char* chunk;
  std::string chunk_string = "";

  int chunk_size = 50000;
  MPI_Offset offset = 0;

  bool found = false;
  int starting_pos;

  while(!found){
    //allocate a chunk
    chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
    // read it in
    MPI_File_read_at(infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
    offset += chunk_size;
    // end the string
    chunk[chunk_size] = '\0';
    std::string new_string(chunk);
    chunk_string = new_string;
    free(chunk);

    //see if the id we want is in there
    char id_str[10];
    sprintf(id_str, "%d", starting_id);
    starting_pos = chunk_string.find(id_str);
    if (starting_pos < chunk_string.size()){
      found = true;
    }
  }

  chunk_string = chunk_string.substr(starting_pos);

  int current_id = starting_id;
  while(current_id < final_id /*&& global_count < 100000*/){
    int next_id = current_id + 1;
    if (next_id == final_id){
      find_friends(current_id, chunk_string);
    }else{
      int found_next;
      while(true){
        char next_id_str[10];
        sprintf(next_id_str, "%d", next_id);
        found_next = chunk_string.find(next_id_str);
        if (found_next < chunk_string.size()){
          break;
        }else{
          chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
          // read it in
          MPI_File_read_at(infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
          offset += chunk_size;
          // end the string
          chunk[chunk_size] = '\0';
          std::string new_string(chunk);
          free(chunk);
          chunk_string += new_string;
        }
      }
      std::string current_chunk = chunk_string.substr(0, found_next);
      find_friends(current_id, current_chunk);
      chunk_string = chunk_string.substr(found_next);
    }
    current_id += 1;
  }

  MPI_File_close(&infile);
}

void find_friends(int id, std::string chunk){
  //printf("%d %s\n", id, chunk.c_str());
  global_count += 1;
  int found = chunk.find(':');
  chunk = chunk.substr(found+1);

  found = chunk.find("\n");
  while (found < chunk.size()){
    chunk = chunk.substr(0, found);
    found = chunk.find("\n");
  }
  if (chunk != "notfound" && chunk != "private" && chunk != ""){
    //printf("%d:  %s\n", id, chunk.c_str());
    g_adj_list[id] = std::set<int>();

    found = chunk.find(',');
    while (found < chunk.size()){
      std::string before = chunk.substr(0, found);

      g_adj_list[id].insert(atoi(before.c_str()));

      chunk = chunk.substr(found+1);
      found = chunk.find(',');
    }
    g_adj_list[id].insert(atoi(chunk.c_str()));
    printf("%d: num_friends: %lu\n", id, g_adj_list[id].size());
  }
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
  //printf("Rank %d:      g_file: %d       g_user_start: %d     g_num_users: %d\n", g_mpi_rank, g_file, g_user_start, g_num_users);
}

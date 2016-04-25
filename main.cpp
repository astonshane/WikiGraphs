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
  //parse_file();
  if (g_mpi_rank < 10){
    parse_file();
  }
  // Close the file
  //MPI_File_close(&infile);

  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void parse_file(){
  printf("parsing the file...\n");
  char filename[27];
  std::string padding;
  if (g_file < 10){
    padding = "00";
  }else if (g_file < 100) {
    padding = "0";
  } else {
    padding = "";
  }

  sprintf(filename, "data/friends-%s%d______.txt", padding.c_str(), g_file);
  printf("rank: %d  %s\n", g_mpi_rank, filename);

  MPI_File infile;
  MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  printf("here again\n");
  MPI_Offset filesize;
  MPI_File_get_size(infile, &filesize);


  std::string starting_id = std::to_string(1000000*g_file + g_user_start);
  int starting_id_int = atoi(starting_id.c_str()); // the first user id that will be done by this rank
  int final_id = starting_id_int + g_num_users; // the last of the user id's that will be done by this rank
  std::cout << starting_id << "  " << final_id << std::endl;

  printf("here0\n");

  int chunk_size = 10000;

  MPI_Offset offset = 0;

  bool found = false;
  int starting_pos;

  char * chunk;
  std::string chunk_string = "";
  printf("here!\n");

  //find the starting point in the file
  while (!found){
    printf("!found\n");
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
    starting_pos = chunk_string.find(starting_id);
    if (starting_pos < chunk_string.size()){
      found = true;
    }
  }

  chunk_string = chunk_string.substr(starting_pos);
  //printf("starting chunk string:\n========= %s\n", chunk_string.c_str());

  // start to make the connections
  int current_id = starting_id_int;
  while (current_id < final_id && offset < filesize){
    int next_id = current_id + 1;
    int found_next = chunk_string.find(std::to_string(next_id));
    if (found_next < chunk_string.size()){
      std::string current_chunk = chunk_string.substr(0, found_next-1);
      find_friends(current_id, current_chunk);
      chunk_string = chunk_string.substr(found_next);
    }else{
      if (offset < filesize){
        chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
        // read it in
        MPI_File_read_at(infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
        offset += chunk_size;
        chunk[chunk_size] = '\0';
        std::string new_string(chunk);
        chunk_string += new_string;
        free(chunk);

        found_next = chunk_string.find(std::to_string(next_id));
        std::string current_chunk = chunk_string.substr(0, found_next-1);
        find_friends(current_id, current_chunk);
        chunk_string = chunk_string.substr(found_next);

      }else{
        find_friends(current_id, chunk_string);
      }
    }
    current_id += 1;
  }


  /*while (true){
   // allocate a chunk to read in from the file
   chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
   // read it in
   MPI_File_read_at(infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
   // end the string
   chunk[chunk_size] = '\0';
   std::string new_string(chunk);
   chunk_string = new_string;
   free(chunk);
   offset += chunk_size-1000;
   std::cout << chunk_string << std::endl;
   unsigned int found_newline = chunk_string.find("\n");

   break;
 }*/

  MPI_File_close(&infile);
}

void find_friends(int id, std::string chunk){
  printf("%d    %s\n", id, chunk.c_str());
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

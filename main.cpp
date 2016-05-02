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
int start_id;
int end_id;
int g_max_id = 125000000;
MPI_Offset file_start;
MPI_Offset file_end;
std::map<int, std::set<int>> g_adj_list;

void parse_file();
void listen();
void add_to_adjlist(std::string line);
int id_to_rank(int id);

int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(&argc, &argv);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

  start_id = g_mpi_rank*(g_max_id / g_world_size);
  end_id = (g_mpi_rank+1)*(g_max_id / g_world_size);
  if (g_mpi_rank == g_world_size-1){
    end_id = g_max_id;
  }

  //printf("Rank %d: start_id: %d end_id: %d\n", g_mpi_rank, start_id, end_id);

  // Print off a hello world message
  parse_file();
  //printf("Rank: %d    g_adj_list.size(): %lu\n", g_mpi_rank, g_adj_list.size());
  if (g_mpi_rank == 0){
   // parse_file();
    //printf("Rank: %d    g_adj_list.size(): %lu\n", g_mpi_rank, g_adj_list.size());
  }



  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void parse_file(){
  char filename[70];
  //sprintf(filename, "com-friendster.ungraph.txt");
  sprintf(filename, "/gpfs/u/home/PCP5/PCP5stns/scratch-shared/com-friendster.ungraph.txt");

  MPI_File infile;
  MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  MPI_Offset filesize = 0;
  MPI_File_get_size(infile, &filesize);

  file_start = (g_mpi_rank)*(filesize/g_world_size);
  if (g_mpi_rank == 0){
    file_start = 124;
  }else{
    file_start -= 50; // allow some overlap to catch newlines
  }
  file_end = (g_mpi_rank+1)*(filesize/g_world_size);
  if (g_mpi_rank == g_world_size-1){
    file_end = filesize;
  }
  /*if (g_mpi_rank != 0){
   MPI_File_close(&infile);
   return;
  }*/
  MPI_Offset offset = file_start;
  char * buffer;
  std::string chunk = "";
  unsigned int buffer_size = 100000;

  bool skip = (g_mpi_rank != 0);

  while (offset < file_end){
   // printf("Rank: %d offset: %lld, file_end: %lld, remaining: %lld  size of adj_list: %lu\n", g_mpi_rank, offset, file_end, file_end-offset, g_adj_list.size());
    buffer = (char *)malloc( (buffer_size + 1)*sizeof(char));
    MPI_File_read_at(infile, offset, buffer, buffer_size, MPI_CHAR, MPI_STATUS_IGNORE);
    offset += buffer_size;
    // end the string
    buffer[buffer_size] = '\0';
    std::string new_string(buffer);
    free(buffer);
    chunk += new_string;

    if (skip){
      // skip up to the first newline to remove any broken lines from starting in the middle of the file
      chunk = chunk.substr(chunk.find('\n')+1);
    }else{
      skip = true;
    }

    int found_nl = chunk.find('\n');
    while (found_nl < chunk.size()){
      std::string line = chunk.substr(0, found_nl);
      add_to_adjlist(line);

      chunk = chunk.substr(found_nl+1);
      found_nl = chunk.find('\n');
    }
    MPI_Barrier(MPI_COMM_WORLD);
    listen();
  }
  printf("Rank %d: finished read in of own portion of file    %lu\n", g_mpi_rank, g_adj_list.size());
  //MPI_Barrier(MPI_COMM_WORLD);
  MPI_File_close(&infile);
}

int id_to_rank(int id){
  int max_id=0;
  for (int i=0; i<g_world_size; i++){
    max_id += g_max_id / g_world_size;
    if (id < max_id){
      return i;
    }
  }
  return g_world_size-1;
}

void isend(int one, int two, int rank){
  int * to_send = (int *)malloc(2*sizeof(int));
  to_send[0] = one;
  to_send[1] = two;
  MPI_Request send;
  MPI_Isend(to_send, 2, MPI_INT, rank, 0, MPI_COMM_WORLD, &send);
  free(to_send);
}

void listen(){
  int flag;
  MPI_Status status;
  MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
  while(flag){
    int source = status.MPI_SOURCE;
    int * to_recv = (int *)malloc(2*sizeof(int));
    MPI_Request req;
    MPI_Irecv(to_recv, 2, MPI_INT, source, 0, MPI_COMM_WORLD, &req);
    //printf("Rank %d recieved %d %d from rank %d\n", g_mpi_rank, to_recv[0], to_recv[1], source);
    g_adj_list[to_recv[0]].insert(to_recv[1]);
    free(to_recv);
    MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
  }
}

void add_to_adjlist(std::string line){

  int tab_pos = line.find("\t");
  int one = atoi(line.substr(0, tab_pos).c_str());
  int two = atoi(line.substr(tab_pos+1).c_str());

  if (one >= start_id && one < end_id){
    g_adj_list[one].insert(two);
    int r2 = id_to_rank(two);
    if (r2 == g_mpi_rank){
      g_adj_list[two].insert(one);
    }else{
      //isend(two, one, r2);
    }
  }else{
    if (two >= start_id && two < end_id){
      g_adj_list[two].insert(one);
      int r1 = id_to_rank(one);
      //isend(one, two, r1);
    }else{
      int r1 = id_to_rank(one);
      int r2 = id_to_rank(two);
      //isend(one, two, r1);
      //isend(two, one, r2);
    }
  }

}

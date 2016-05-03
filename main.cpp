#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>

int g_world_size;
int g_mpi_rank;
int start_id;
int end_id;
int g_max_id = 125000000;
int count = 0;
MPI_Offset file_start;
MPI_Offset file_end;
std::map<int, std::vector<int>> g_adj_list;

void parse_file_one_rank();
void parse_file();
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

  printf("Rank %d: start_id: %d end_id: %d\n", g_mpi_rank, start_id, end_id);

  // Print off a hello world message
  parse_file();   
  // if(g_mpi_rank==0){
  //   parse_file_one_rank();
  // }

  MPI_Barrier(MPI_COMM_WORLD);
  printf("Rank: %d    g_adj_list.size(): %lu    elements in list: %lu\n", g_mpi_rank, g_adj_list.size(), count);
  //if (g_mpi_rank == 0){
   // parse_file();
    //printf("Rank: %d    g_adj_list.size(): %lu\n", g_mpi_rank, g_adj_list.size());
  // }



  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

//only use this once to optimize file parsing
void parse_file_one_rank(){
  std::string line;
  int count = 0;
  std::ifstream infile("com-friendster.ungraph.txt");
  for(int i =0; i<4; i++){
    std::getline(infile, line);
  }
  std::vector<std::ofstream*> streams;
  for(int i=0;i<125;i++) {
    std::stringstream sstm;
    sstm<<"./Friendster/users_"<<i<<".txt";
    std::string fileName = sstm.str();
    streams.push_back(new std::ofstream(fileName.c_str(), std::ofstream::out));
  }
  while(std::getline(infile,line)){
    count++;
    int tab_pos = line.find("\t");
    int one = atoi(line.substr(0, tab_pos).c_str());
    int two = atoi(line.substr(tab_pos+1).c_str());
    int oneFile = floor((double)one/1000000);
    int twoFile = floor((double)two/1000000);
    std::stringstream sstm;
    sstm<<one<<"\t"<<two<<"\n";
    std::string lineOne = sstm.str();
    sstm.str(std::string());
    sstm.clear();
    sstm<<two<<"\t"<<one<<"\n";
    std::string lineTwo = sstm.str();
    
    *streams[oneFile]<<lineOne;
    *streams[twoFile]<<lineTwo;
    if(count%500000==0){
      double percent = one/(double)g_max_id*100;
      std::cout<<percent<<"%"<<std::endl;
    }
  }
}


void parse_file(){
  int lowFile = floor((double)start_id/1000000);
  int highFile = floor((double)end_id/1000000);
  for (int i=0; i<highFile-lowFile+1; i++){
    std::stringstream sstm;
    sstm<<"/gpfs/u/home/PCP5/PCP5bhgw/scratch/users_"<<lowFile<<".txt";
    std::ifstream infile(sstm.str().c_str());
    std::string line;
    while(std::getline(infile,line)){
      add_to_adjlist(line);  
    }
    //MPI_Barrier(MPI_COMM_WORLD);
    lowFile++;
    sstm.str(std::string());
    sstm.clear();
  }
  printf("Rank %d: finished read in of own portion of file    %lu\n", g_mpi_rank, g_adj_list.size());
    //sprintf(filename, "com-friendster.ungraph.txt");
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

void add_to_adjlist(std::string line){
  int tab_pos = line.find("\t");
  int one = atoi(line.substr(0, tab_pos).c_str());
  int two = atoi(line.substr(tab_pos+1).c_str()); 
  //std::cout<<start_id<<" "<<end_id<<std::endl;
  if (one >= start_id && one < end_id){
    g_adj_list[one].push_back(two);
    count++;
    if(count%1000000==0){
      std::cout<<g_mpi_rank<<" "<<count<<std::endl;
    }
  }
}

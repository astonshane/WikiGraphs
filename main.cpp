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
#include <list>
#include<hwi/include/bqc/A2_inlines.h>

// global variables
int g_world_size;
int g_mpi_rank;
int start_id;
int end_id;
const int g_max_id = 3072441;
int count = 0;
MPI_Offset file_start;
MPI_Offset file_end;
std::map<int, std::vector<int> > g_adj_list;
bool * visited = (bool *)calloc(g_max_id, sizeof(bool));
bool * inQueue = (bool *)calloc(g_max_id, sizeof(bool));
std::vector<std::list<int> > connectedComponents;
std::map<int, std::vector<int> > boundaryEdges;

unsigned long long start_parse_time=0;
unsigned long long end_parse_time=0;
unsigned long long start_cc_time=0;
unsigned long long end_cc_time=0;

// function declarations
void parse_file_one_rank();
void parse_file();
int add_to_adjlist(std::string line);
int id_to_rank(int id);
MPI_Offset compute_offset(char * filename);
void bfs(int u, int ccCounter);


//////////////////////////////////////
int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(&argc, &argv);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

  start_parse_time = GetTimeBase();

  start_id = g_mpi_rank*(g_max_id / g_world_size);
  end_id = (g_mpi_rank+1)*(g_max_id / g_world_size);
  if (g_mpi_rank == g_world_size-1){
    end_id = g_max_id;
  }

  parse_file();
  end_parse_time = GetTimeBase();

  printf("Parse Time (raw timer): %llu\n", end_parse_time - start_parse_time);
  printf("Parse Time (seconds?): %f\n", float(end_parse_time - start_parse_time)/float(1600000000));

  start_cc_time = GetTimeBase();
  for(int i=0; i<g_max_id; i++){
    visited[i]=false;
    inQueue[i]=false;
  }

  for(std::map<int, std::vector<int> >::iterator itr =g_adj_list.begin(); itr!=g_adj_list.end(); itr++){
    visited[itr->first]=true;
  }

  for(std::map<int, std::vector<int> >::iterator itr =g_adj_list.begin(); itr!=g_adj_list.end(); itr++) {
    std::vector<int> temp = itr->second;
    for(int i =0; i<temp.size(); i++){
      if(!visited[temp[i]]){
        if(g_adj_list.find(temp[i])==g_adj_list.end()){
          std::vector<int> temp1;
          temp1.push_back(itr->first);
          g_adj_list[temp[i]] = temp1;
        }
        else{
          g_adj_list[temp[i]].push_back(itr->first);
        }
      }
    }
  }

  for(std::map<int, std::vector<int> >::iterator itr =g_adj_list.begin(); itr!=g_adj_list.end(); itr++){
    visited[itr->first]=false;
  }

  int ccCounter = -1;
  for(std::map<int, std::vector<int> >::iterator itr =g_adj_list.begin(); itr!=g_adj_list.end(); itr++) {
    if(!visited[itr->first]) {
      ccCounter += 1;
      std::list<int> temp;
      connectedComponents.push_back(temp);
      bfs(itr->first, ccCounter);
    }
  }

  printf("Rank: %d    g_adj_list.size(): %lu    elements in list: %d    CCs: %d\n", g_mpi_rank, g_adj_list.size(), count, ccCounter+1);
  free(visited);
  free(inQueue);
  end_cc_time = GetTimeBase();

  MPI_Barrier(MPI_COMM_WORLD);

  printf("CC Time: %llu\n", end_cc_time - start_cc_time);
  printf("CC Time (seconds?): %f\n", float(end_cc_time - start_cc_time)/float(1600000000));

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
  }
}


MPI_Offset compute_offset(char * filename){
  MPI_File infile;
  MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  MPI_Offset filesize = 0;
  MPI_File_get_size(infile, &filesize);

  MPI_Offset offset = 0;
  int num_sections = 100;
  for (int i=1; i<num_sections; i++){
    // set the temp offset
    int tmp_offset = (filesize / num_sections) * i;

    // allocate some space to read in at the offset
    char * buffer = (char *)malloc( (1001)*sizeof(char));
    // read in there
    MPI_File_read_at(infile, tmp_offset, buffer, 1000, MPI_CHAR, MPI_STATUS_IGNORE);
    buffer[1000] = '\0';
    // cast as a string
    std::string chunk(buffer);
    free(buffer);
    //std::cout << chunk << std::endl;

    // read up to the first newline and throw that away
    chunk = chunk.substr(chunk.find('\n')+1);
    // get everything from there to the next newline
    std::string line = chunk.substr(0, chunk.find('\n'));

    // split it up over the tab and pull out the first id
    int tab_pos = line.find("\t");
    int first_id = atoi(line.substr(0, tab_pos).c_str());
    // if this id is greater than the starting id, the offset is too big, go to the last one
    if (first_id > start_id){
      offset = (filesize / num_sections) * (i-1);
      break;
    }
  }
  return offset;
}

void parse_file(){
  int lowFile = floor((double)start_id/24600);
  int highFile = floor((double)end_id/24600);
  // loop through all of the files that will hold information about our set of ids
  for (int i=0; i<highFile-lowFile+1; i++){
    // create the filename as a c_str
    char filename[100];
    sprintf(filename, "/gpfs/u/home/PCP5/PCP5stns/scratch/user_%d.txt", lowFile);

    // open the file and get the filesize
    MPI_File infile;
    MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
    MPI_Offset filesize = 0;
    MPI_File_get_size(infile, &filesize);

    //printf("%s: %lu", filename, filesize);

    // start the offset at 0, for now
    MPI_Offset offset = compute_offset(filename);

    bool skip = true;
    if(offset==0){
      skip = false;
    }
    char * buffer;
    std::string chunk = "";
    // set the buffer size, ie. how much to read in at a time
    unsigned int buffer_size = 10000;

    // keep track of the latest id that we read in so that we can stop reading if we've found the last id that we need
    int latest_id_found = 0;

    // loop while we haven't read the whole file and we haven't found the latest id yet
    while (offset < filesize && latest_id_found < end_id){
      // printf("Rank: %d offset: %lld, file_end: %lld, remaining: %lld  size of adj_list: %lu\n", g_mpi_rank, offset, file_end, file_end-offset, g_adj_list.size());
      // read in a chukn to the buffer
      buffer = (char *)malloc( (buffer_size + 1)*sizeof(char));
      MPI_File_read_at(infile, offset, buffer, buffer_size, MPI_CHAR, MPI_STATUS_IGNORE);
      offset += buffer_size-100;
      // end the string
      buffer[buffer_size] = '\0';
      // make the c_string a std::string for convenience
      std::string new_string(buffer);
      free(buffer);
      // add the new string to the existing one
      chunk += new_string;

      if (skip){
        // skip up to the first newline to remove any broken lines from starting in the middle of the file
        chunk = chunk.substr(chunk.find('\n')+1);
      }else{
        skip = true;
      }

      // find the first newline in the file
      int found_nl = chunk.find('\n');
      // loop while there are still newlines and we havne't found the last id yet
      while (found_nl < chunk.size() && latest_id_found < end_id){
        // get the part of the string before the newline
        std::string line = chunk.substr(0, found_nl);
        // parse that part of it
        latest_id_found = add_to_adjlist(line);

        // reset the string so it is past that first part
        chunk = chunk.substr(found_nl+1);
        // search for another newline
        found_nl = chunk.find('\n');
      }
   }
   // go to the next file
   lowFile++;
  }
  //printf("Rank %d: finished read in of own portion of file    %lu\n", g_mpi_rank, g_adj_list.size());
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

int add_to_adjlist(std::string line){
  int tab_pos = line.find("\t");
  int one = atoi(line.substr(0, tab_pos).c_str());
  int two = atoi(line.substr(tab_pos+1).c_str());
  //std::cout<<start_id<<" "<<end_id<<std::endl;
  if (one >= start_id && one < end_id){
    g_adj_list[one].push_back(two);
    count++;
  }
  return one;
}

void bfs(int u, int ccCounter) {
  std::list<int> queue;
  visited[u] = true;
  queue.push_back(u);
  int prev = u;
  while(!queue.empty()) {
    int s=queue.front();
    visited[s]=true;
    connectedComponents[ccCounter].push_back(s);
    queue.pop_front();
    std::map<int, std::vector<int> >::iterator adj_list_itr;
    adj_list_itr = g_adj_list.find(s);
    if(adj_list_itr->second.size() == 1) {
      std::map<int, std::vector<int> >::iterator it = boundaryEdges.find(prev);
      if(it != boundaryEdges.end()){
        it->second.push_back(s);
      }
      else{
        std::vector<int> temp;
        temp.push_back(s);
        boundaryEdges[prev] = temp;
      }
    }
    else {
      for(std::vector<int>::iterator itr = adj_list_itr->second.begin(); itr != adj_list_itr->second.end(); itr++) {
        if(!visited[*itr]){
          if(inQueue[*itr]) {
            continue;
          }
          else{
            queue.push_back(*itr);
            inQueue[*itr] = true;
          }
        }
      }
    }
    prev = s;
  }
}

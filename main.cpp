#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <climits>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <list>
#include <algorithm>
#include <utility>
#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>
#include<hwi/include/bqc/A2_inlines.h>

/* ========== variable declarations ========== */
int g_world_size; // number of overall ranks
int g_mpi_rank; // the current rank
int start_id; // the minimum id that this rank covers
int end_id; // the maximum id that this rank covers
const int g_max_id = 3072441; // the highest user id found in the orkut dataset

int count = 0; // count of the edges added to the adjacency list (for debugging)

MPI_Offset file_start; // where in the file this rank will start
MPI_Offset file_end; // where in the file this rank will end

std::map<int, std::vector<int> > g_adj_list; // the adjacency list to store the undirected graph

bool * visited = (bool *)calloc(g_max_id, sizeof(bool)); // visited list for bfs
bool * inQueue = (bool *)calloc(g_max_id, sizeof(bool)); // queue for bfs

std::list<std::set<int> > connectedComponents; // the connected connected components themselves
std::list<std::set<int> > outgoingEdges; // the connected connected components themselves

// timer variables
unsigned long long start_parse_time=0;
unsigned long long end_parse_time=0;
unsigned long long start_cc_time=0;
unsigned long long end_cc_time=0;
unsigned long long start_merge_time=0;
unsigned long long end_merge_time=0;


/* ========== function declarations ========== */
void parse_file_one_rank();
void parse_file();
int add_to_adjlist(std::string line);
MPI_Offset compute_offset(char * filename);
void bfs(int u, std::list<std::set<int> >::iterator cc_iter, std::list<std::set<int> >::iterator oe_iter);
/* ========== main function ========== */
int main(int argc, char** argv) {
  // Initialize the MPI environment
  int ccCounter = -1;
  MPI_Init(&argc, &argv);
  int bracket_size;
  int gap;

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

  start_parse_time = GetTimeBase(); // start the parsing timer

  // get the first id for this rank
  start_id = g_mpi_rank*(g_max_id / g_world_size);
  //get the max id for this rank
  end_id = (g_mpi_rank+1)*(g_max_id / g_world_size);
  // the last id will get a few extra ids to cover the extra in a case that it doesn't divide evenly
  if (g_mpi_rank == g_world_size-1){
    end_id = g_max_id;
  }

  parse_file(); // parse the file(s) to read in the graph data

  end_parse_time = GetTimeBase(); // end the parsing timer

  // only print out the timer info for rank 0
  if (g_mpi_rank == 0){
    printf("Parse Time (raw timer): %llu\n", end_parse_time - start_parse_time);
    printf("Parse Time (seconds?): %f\n", float(end_parse_time - start_parse_time)/float(1600000000));
  }

  start_cc_time = GetTimeBase(); // start the CC timer

  //set all nodes to not visited 
  for(int i=0; i<g_max_id; i++){
    visited[i]=false;
    inQueue[i]=false;
  }

  //create a reverse graph adj_list and add to map so connections are not lost
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

  // Run BFS on each component in the adj_list
  for(std::map<int, std::vector<int> >::iterator itr =g_adj_list.begin(); itr!=g_adj_list.end(); itr++) {
    if(!visited[itr->first]) {
      ccCounter += 1;
      std::set<int> temp, temp2;
      connectedComponents.push_back(temp);
      outgoingEdges.push_back(temp2);
      bfs(itr->first, (--connectedComponents.end()), (--outgoingEdges.end()));
    }
  }

  printf("Rank: %d    g_adj_list.size(): %lu    elements in list: %d    CCs: %d\n", g_mpi_rank, g_adj_list.size(), count, ccCounter+1);

  // Free memory only used by BFS
  free(visited);
  free(inQueue);
  MPI_Barrier(MPI_COMM_WORLD);
  end_cc_time = GetTimeBase();

  if (g_mpi_rank == 0){
    printf("CC Time: %llu\n", end_cc_time - start_cc_time);
    printf("CC Time (seconds?): %f\n", float(end_cc_time - start_cc_time)/float(1600000000));
  }

  start_merge_time = GetTimeBase();
  // Finalize the MPI environment.

  gap = 1; // gap between merging ranks
  int num_cc;
  MPI_Status stat;
  int out_size,cc_size;
  bracket_size = g_world_size;

  while(bracket_size > 1){

    if((g_mpi_rank/gap)%2 == 0){ //recieves
      printf("Recieving Rank %d\n", g_mpi_rank);
      MPI_Recv(&num_cc, 1, MPI_INT, g_mpi_rank+gap, 1,MPI_COMM_WORLD, &stat); //recieve number of ccs
      for(int j = 0; j < num_cc; j++){
        MPI_Recv(&cc_size, 1, MPI_INT, g_mpi_rank+gap, 4*j, MPI_COMM_WORLD, &stat); //recieve size of individual cc
        int incoming_cc[cc_size];
        MPI_Recv(incoming_cc, cc_size, MPI_INT, g_mpi_rank+gap, 4*j + 1, MPI_COMM_WORLD, &stat); //recieve cc as array
        std::set<int> new_cc(incoming_cc, incoming_cc+cc_size);
        MPI_Recv(&out_size, 1, MPI_INT, g_mpi_rank+gap, 4*j+2, MPI_COMM_WORLD, &stat); //recieve size of individual cc
        int in_edges[out_size];
        MPI_Recv(in_edges, out_size, MPI_INT, g_mpi_rank+gap, 4*j + 3, MPI_COMM_WORLD, &stat); //recieve cc as array
        std::set<int> new_oe;

        for(int k = 0; k < out_size; ++k){
          std::list<std::set<int> >::iterator oe_iter = outgoingEdges.begin();
          for(std::list<std::set<int> >::iterator cc_iter = connectedComponents.begin(); cc_iter != connectedComponents.end(); ++cc_iter){
            if(cc_iter->find(in_edges[k]) != cc_iter->end()){
                new_cc.insert(cc_iter->begin(), cc_iter->end());
                for(std::set<int>::iterator out_edge_trim = oe_iter->begin(); out_edge_trim != oe_iter->end(); ++out_edge_trim){
                  if(new_cc.find(*out_edge_trim) == new_cc.end()){
                    new_oe.insert(*out_edge_trim);
                  }
                }
                connectedComponents.erase(cc_iter++);
                outgoingEdges.erase(oe_iter++);
            }else{
                new_oe.insert(in_edges[k]);
                oe_iter++;
            }
            if(connectedComponents.size() == 0) break;
          }
        }
        connectedComponents.push_back(new_cc);
        outgoingEdges.push_back(new_oe);
      }
    }else if((g_mpi_rank/gap)%2 == 1){ //sends
      num_cc = connectedComponents.size();
      MPI_Send(&num_cc, 1, MPI_INT, g_mpi_rank-gap, 1,MPI_COMM_WORLD); //sends number of ccs
      int j =0;
      std::list<std::set<int> >::iterator oe_iter = outgoingEdges.begin();
      for(std::list<std::set<int> >::iterator cc_iter = connectedComponents.begin(); cc_iter != connectedComponents.end(); ++cc_iter){
        cc_size = cc_iter->size();
        MPI_Send(&cc_size, 1, MPI_INT, g_mpi_rank-gap, 4*j, MPI_COMM_WORLD); //sends size of individual cc
        int outgoing_cc[cc_size];
        int i =0;
        for(std::set<int>::iterator iter = cc_iter->begin(); iter != cc_iter->end(); ++iter){ //convert cc to array
          outgoing_cc[i] = *iter;
          ++i;
        }
        MPI_Send(outgoing_cc, cc_size, MPI_INT, g_mpi_rank-gap, 4*j + 1, MPI_COMM_WORLD); //send cc as array

        out_size = oe_iter->size();
        MPI_Send(&out_size, 1, MPI_INT, g_mpi_rank-gap, 4*j+2, MPI_COMM_WORLD); //sends size of outgoing edges
        int out_edges[out_size];
        i =0;
        for(std::set<int>::iterator outiter = oe_iter->begin(); outiter != oe_iter->end(); ++outiter){ //convert edges to array
          out_edges[i] = *outiter;
          ++i;
        }
        MPI_Send(out_edges, out_size, MPI_INT, g_mpi_rank-gap, 4*j + 3, MPI_COMM_WORLD); //send edges as array;
        ++j;
        ++oe_iter;
      }
      break;
    }else{
     break;
   }

   bracket_size = bracket_size/2; 
   gap*=2; 
 }
 MPI_Barrier(MPI_COMM_WORLD);
 end_merge_time = GetTimeBase();

 if (g_mpi_rank == 0){
  printf("Merge Time: %llu\n", end_cc_time - start_cc_time);
  printf("Merge Time (seconds?): %f\n", float(end_cc_time - start_cc_time)/float(1600000000));
}

MPI_Finalize();

return 0;
}

/* ========== compute_offset() ========== */
/*
    * given a file and the starting id for the rank, determine the 'best' place in the file to start reading at for parsing
    * break the file into 100 sections
    * read just the first part of each of those sections in order
    * once a user id is found at one of these sections that is greater than the starting id, return the previous section to start parsing
*/
    MPI_Offset compute_offset(char * filename){
  // open the file & get the filesize
      MPI_File infile;
      MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
      MPI_Offset filesize = 0;
      MPI_File_get_size(infile, &filesize);

      MPI_Offset offset = 0;
  int num_sections = 100; // number of sections to use
  for (int i=1; i<num_sections; i++){
    // set the temp offset to test at
    int tmp_offset = (filesize / num_sections) * i;

    // allocate some space to read in at the offset
    char * buffer = (char *)malloc( (1001)*sizeof(char));
    // read in at this offset
    MPI_File_read_at(infile, tmp_offset, buffer, 1000, MPI_CHAR, MPI_STATUS_IGNORE);
    buffer[1000] = '\0';
    std::string chunk(buffer); // cast as a string for convenience
    free(buffer); // free the memory

    // read up to the first newline and throw that away (becuase the read started at a random part of the file)
    chunk = chunk.substr(chunk.find('\n')+1);

    // get everything from there to the next newline
    std::string line = chunk.substr(0, chunk.find('\n'));

    // split it up over the tab
    int tab_pos = line.find("\t");

    //pull out the first id
    int first_id = atoi(line.substr(0, tab_pos).c_str());

    // if this id is greater than the starting id, the offset is too big, go to the last one
    if (first_id > start_id){
      offset = (filesize / num_sections) * (i-1);
      break;
    }
  }
  return offset;
}

/* ========== parse_file() ========== */
/*
    * parse the dataset from the file (or files)
    * read in each edge and put it into the adjacency list
*/
    void parse_file(){
  // determine the first and last files that this rank needs to read from to get all of its info
      int lowFile = floor((double)start_id/24600);
      int highFile = floor((double)end_id/24600);

  // loop through all of the files that will hold information about our set of ids
      for (int i=0; i<highFile-lowFile+1; i++){
    // construct the appropriate filename
        char filename[100];
        sprintf(filename, "/gpfs/u/home/PCP5/PCP5stns/scratch/user_%d.txt", lowFile);

    // open the file and get the filesize
        MPI_File infile;
        MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
        MPI_Offset filesize = 0;
        MPI_File_get_size(infile, &filesize);

    // calculate the optimal offset
        MPI_Offset offset = compute_offset(filename);

    // if the offset isn't 0, the first read needs to be trimmed at the front becuase the rank starts at a random spot in the file
        bool skip = true;
        if(offset==0){
          skip = false;
        }

    // declare some variables to use
        char * buffer;
        std::string chunk = "";

    // set the buffer size, ie. how much to read in at a time
        unsigned int buffer_size = 10000;

    // keep track of the latest id that we read in so that we can stop reading if we've found the last id that we need
        int latest_id_found = 0;

    // loop while we haven't read the whole file and we haven't found the last id yet
        while (offset < filesize && latest_id_found < end_id){
      // read in a chunk of the file to the buffer
          buffer = (char *)malloc( (buffer_size + 1)*sizeof(char));
          MPI_File_read_at(infile, offset, buffer, buffer_size, MPI_CHAR, MPI_STATUS_IGNORE);

      // update the offset (subtract 100 to catch the overlap from skipping parts at the begining)
          offset += buffer_size-100;

      // end the string
          buffer[buffer_size] = '\0';

      // make the c_string a std::string for convenience
          std::string new_string(buffer);
          free(buffer);

      // add the new string to the existing one
          chunk += new_string;

      // skip up to the first newline to remove any broken lines from starting in the middle of the file
          if (skip){
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
    }

/* ========== add_to_adjlist() ========== */
/*
    * take a line from the file and add the edge to the adjacency list
    * returns the id that it found
*/
int add_to_adjlist(std::string line){
  int tab_pos = line.find("\t"); // get the tab position
  int one = atoi(line.substr(0, tab_pos).c_str()); // the first user id
  int two = atoi(line.substr(tab_pos+1).c_str()); // the second user id
  // if this id is in the rank's range add it to the adjacency list
  if (one >= start_id && one < end_id){
    g_adj_list[one].push_back(two);
    count++;
  }
  return one;
}


/* ========== bfs() ========== */
/*
    * take a node and perform bfs on it to find all connected components
*/
    void bfs(int u, std::list<std::set<int> >::iterator cc_iter, std::list<std::set<int> >::iterator oe_iter){
  std::list<int> queue;     //create a queue
  visited[u] = true;
  queue.push_back(u);
  while(!queue.empty()) {   //while the queue is not empty
    int s=queue.front();    //pop off the front element and mark as visited
    visited[s]=true;
    cc_iter->insert(s);
    queue.pop_front();
    std::map<int, std::vector<int> >::iterator adj_list_itr;
    adj_list_itr = g_adj_list.find(s);

    //find all adj nodes and put in queue if not already visited or in queue
    if(adj_list_itr->second.size() != 1) {
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
    }else{
     oe_iter->insert(s);
   }	
 }
}


/* ========== pasrse_file_one_rank() ========== */
/*
  * take the one large file from SNAP into 125 smaller files for ease of parsing
  * only use this once to optimize file parsing
*/
  void parse_file_one_rank(){
    std::string line;
    int count = 0;
    std::ifstream infile("com-orkut.ungraph.txt");
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

#include <stdio.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>

typedef std::map<std::string, std::set<std::string>> LinkMap;

int g_world_size;
int g_my_rank;
std::string g_filename = "enwiki-20160305-pages-articles.xml";

void parse_file(MPI_File *infile);
void regex_test();
void find_links(std::string section, std::string current_title, LinkMap &links);


int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_my_rank);

  // Print off a hello world message
  printf("Hello world from rank %d out of %d processors\n", g_my_rank, g_world_size);

  // Open up the wikipedia xml file
  MPI_File infile;
  MPI_File_open(MPI_COMM_WORLD, (char *)g_filename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);

  // Parse the file
  parse_file(&infile);

  if (g_my_rank == 0){
    //regex_test();
  }

  // Close the file
  MPI_File_close(&infile);

  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

// Parse the input file: pull out page titles and any links from a page to another
void parse_file(MPI_File *infile){
  //get file size
  MPI_Offset filesize;
  MPI_File_get_size(*infile, &filesize);

  // the size of each chunk that will be read in at a time
  //  TODO: find an optimal setting for this so that we aren't producing too many read reqeusts
  int chunk_size = 1000;
  //get size of the chunk this rank will handle
  long long int max_size = filesize / g_world_size;
  // where this rank will stop reading the file (aka where the next rank will start reading)
  long long int stopping_point = max_size * (g_my_rank + 1);

  //get the start point for this rank in the file:
  //    subtract chunk_size from the 'logical' start point to make sure
  //    that each node has a title for the links that it finds
  MPI_Offset offset = std::max(max_size * g_my_rank, max_size * g_my_rank - chunk_size);

  printf("Rank %d ==> start point: %lld\n", g_my_rank, offset);

  // declare some variables
  char * chunk;
  std::string current_title = "";
  LinkMap links;

  // keep going until the offset is in the next ranks group
  while (offset < stopping_point && links.size() < 10000){
    // allocate a chunk to read in from the file
    chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
    // read it in
    MPI_File_read_at(*infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
    // end the string
    chunk[chunk_size] = '\0';
    // make it a c++ string for convenience
    std::string chunk_string(chunk);
    // free the allocated space
    free(chunk);
    

    unsigned int found_begin, found_end;

    found_begin = chunk_string.find("<title>");
    found_end = chunk_string.find("</title>");
    while(found_begin < chunk_string.size() && found_end < chunk_string.size()){
      if (found_end < found_begin){
	chunk_string = chunk_string.substr(found_end + 7);
        found_begin = chunk_string.find("<title>");
        found_end = chunk_string.find("</title>");
        continue;
      }
      if (current_title != ""){
        std::string prefix = chunk_string.substr(0, std::max((unsigned int)0, found_begin - 1));
        find_links(prefix, current_title, links);
      }

      current_title =  chunk_string.substr(found_begin+7, found_end-found_begin-7);
      links[current_title] = std::set<std::string>();
      chunk_string = chunk_string.substr(found_end+7);
      found_begin = chunk_string.find("<title>");
      found_end = chunk_string.find("</title>");
    }
    if (current_title != ""){
      find_links(chunk_string, current_title, links);
    }

    offset += chunk_size;
  }


  //output the contents of the links map after parsing
  std::cout << links.size() << std::endl;
  for (const auto &key : links) {
    std::cout << key.first << "  " << key.second.size() << std::endl;
    for (const auto &val: key.second){
      std::cout << "    " << val << std::endl;
    }
  }

}

void find_links(std::string section, std::string current_title, LinkMap &links){
  unsigned int found_begin, found_end;

  found_begin = section.find("[[");
  found_end = section.find("]]");
  while(found_begin < section.size() && found_end < section.size()){
    if (found_end > found_begin){
      std::string link = section.substr(found_begin+2, found_end - found_begin - 2);
      if (link.substr(0, 5) != "File:" && link.substr(0, 6) != "Image:"){
        links[current_title].insert(link);
      }
    }
    section = section.substr(found_end+2);
    found_begin = section.find("[[");
    found_end = section.find("]]");
  }
}

void regex_test(){

  std::string chunk_string ("this <title>abc test def</title><title>banana</title>has a submarine as <title>another title</title> subsequence");

  unsigned int found_begin, found_end;

  found_begin = chunk_string.find("<title>");
  found_end = chunk_string.find("</title>");
  while(found_begin != std::string::npos && found_end != std::string::npos){

    std::string new_title = chunk_string.substr(found_begin+7, found_end-found_begin-7);
    std::cout << "new title: " << new_title << std::endl;
    std::cout << found_begin - 1 << std::endl;
    std::string prefix = chunk_string.substr(0, std::max((unsigned int)0, found_begin - 1));
    std::cout << "prefix: " << prefix << std::endl;


    chunk_string = chunk_string.substr(found_end+8);
    std::cout << "new search string: " << chunk_string << std::endl;
    std::cout << std::endl;
    found_begin = chunk_string.find("<title>");
    found_end = chunk_string.find("</title>");
  }

}

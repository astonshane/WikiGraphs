#include <stdio.h>
#include <mpi.h>
#include <boost/regex.hpp>
#include <regex>
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
  //parse_file(&infile);

  if (g_my_rank == 0){
    regex_test();
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

  // the regex to find a tile in the file
  boost::regex title_regex("<title[^>]*>([^<]+)</title>");

  // declare some variables
  char * chunk;
  std::string current_title = "";
  LinkMap links;

  // keep going until the offset is in the next ranks group
  while (offset < stopping_point && links.size() < 100){
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
    //std::cout << "reading" << std::endl;
    //std::cout << chunk_string << std::endl;
    boost::smatch title_match;
    // keep looping while there is another title in the the current chunk
    while (boost::regex_search(chunk_string, title_match, title_regex)){
      // if we've already found at least one title, search the first part of the chunk for any links
      if (current_title != ""){
        find_links(title_match.prefix(), current_title, links);
      }

      // overwrite current_title with the new one
      current_title = title_match[1].str();
      // initilize the new title's spot in the links map with an empty set
      links[current_title] = std::set<std::string>();

      printf("title %lu: %s\n", links.size(), current_title.c_str());
      //std::cout << title_match[0] << std::endl;

      // set the chunk_string to be everything after the title
      chunk_string = title_match.suffix();
    }
    // do one last search through the remainder of the chunk for any links
    if (current_title != ""){
      find_links(chunk_string, current_title, links);
    }
    offset += chunk_size;
  }

  //output the contents of the links map after parsing
  std::cout << links.size() << std::endl;
  for (const auto &key : links) {
    std::cout << key.first << "  " << key.second.size() << std::endl;
    /*for (const auto &val: key.second){
      std::cout << "    " << val << std::endl;
    }*/
  }

}

void find_links(std::string section, std::string current_title, LinkMap &links){
  //links[current_title].push_back("test");
  //std::cout << section << std::endl;
  boost::regex link_regex("\\[\\[([^\\[\\]]*)\\]\\]");
  boost::smatch link_match;
  while (boost::regex_search(section, link_match, link_regex)){
    boost::ssub_match sub_match = link_match[1];
    std::string piece = sub_match.str();
    //std::cout << piece << '\n';
    links[current_title].insert(piece);
    //std::cout << current_title << " > " << piece << std::endl;
    section = link_match.suffix().str();
  }
}

void regex_test(){

  std::string chunk_string ("this <title>abc test def</title><title>banana</title>has a submarine as <title>another title</title> subsequence");
  /*std::cout << chunk_string << std::endl;
  std::size_t found_start = chunk_string.find("<title>");
  std::size_t found_end = chunk_string.find("</title>");
  std::string title = chunk_string.substr(found_start+7, found_end-found_start-7);
  std::cout << found_start << std::endl;
  std::cout << found_end << std::endl;
  std::cout << title << std::endl;;*/

  int found_begin, found_end;

  found_begin = chunk_string.find("<title>");
  found_end = chunk_string.find("</title>");
  while(found_begin != std::string::npos && found_end != std::string::npos){

    std::string new_title = chunk_string.substr(found_begin+7, found_end-found_begin-7);
    std::cout << "new title: " << new_title << std::endl;
    std::cout << found_begin - 1 << std::endl;
    std::string prefix = chunk_string.substr(0, std::max(0, found_begin - 1));
    std::cout << "prefix: " << prefix << std::endl;


    chunk_string = chunk_string.substr(found_end+8);
    std::cout << "new search string: " << chunk_string << std::endl;
    std::cout << std::endl;
    found_begin = chunk_string.find("<title>");
    found_end = chunk_string.find("</title>");
  }

}

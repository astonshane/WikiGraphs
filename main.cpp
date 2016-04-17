#include <stdio.h>
#include <mpi.h>
#include <regex>
#include <iostream>
#include <string>
#include <map>
#include <algorithm>

int g_world_size;
int g_my_rank;
std::string g_filename = "enwiki-20160305-pages-articles.xml";

void parse_file(MPI_File *infile);
void regex_test();
void find_links(std::string section, std::string current_title, std::map<std::string, std::vector<std::string>> &links);


int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Get the number of processes
  MPI_Comm_size(MPI_COMM_WORLD, &g_world_size);

  // Get the rank of the process
  MPI_Comm_rank(MPI_COMM_WORLD, &g_my_rank);

  // Print off a hello world message
  printf("Hello world from rank %d out of %d processors\n", g_my_rank, g_world_size);

  MPI_File infile;
  MPI_File_open(MPI_COMM_WORLD, g_filename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &infile);
  parse_file(&infile);

  if (g_my_rank == 0){
    //regex_test();
  }

  MPI_File_close(&infile);
  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

void parse_file(MPI_File *infile){
  //get file size
  MPI_Offset filesize;
  MPI_File_get_size(*infile, &filesize);

  //get size of the chunk this rank will handle
  long long int max_size = filesize / g_world_size;

  //get the start point for this rank in the file
  MPI_Offset offset = std::max(max_size * g_my_rank, max_size * g_my_rank - 1000);

  printf("Rank %d ==> filesize: %lld ==> chunk size: %lld ==> start point: %lld\n", g_my_rank, filesize, max_size, offset);

  std::regex title_regex("<title>(.+)</title>");

  int chunk_size = 1000;
  char * chunk;
  std::string current_title;

  std::map<std::string, std::vector<std::string>> links;

  while (links.size() < 500){
    chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
    MPI_File_read_at(*infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
    chunk[chunk_size] = '\0';
    //printf("read in: %s", chunk);
    std::string chunk_string(chunk);
    free(chunk);

    std::smatch title_match;
    if (std::regex_search(chunk_string, title_match, title_regex)){
      /*for (size_t i = 0; i < title_match.size(); ++i) {
          std::ssub_match sub_match = title_match[i];
          std::string piece = sub_match.str();
          std::cout << "  submatch " << i << ": " << piece << '\n';
      }*/
      std::string prefix = title_match.prefix();
      find_links(prefix, current_title, links);

      current_title = title_match[1].str();
      links[current_title] = std::vector<std::string>();

      std::string suffix = title_match.suffix();
      //std::cout << "new page:" << current_title << std::endl;
      find_links(suffix, current_title, links);
    }else{
      find_links(chunk_string, current_title, links);
    }

    offset += chunk_size;
  }

  for (const auto &key : links) {
    std::cout << g_my_rank << "  "<< key.first << std::endl;
  }

}

void find_links(std::string section, std::string current_title, std::map<std::string, std::vector<std::string>> &links){
  //links[current_title].push_back("test");
  //std::cout << section << std::endl;
  std::regex link_regex("\\[\\[([^ ]*)\\]\\]");
  std::smatch link_match;
  while (std::regex_search(section, link_match, link_regex)){
    std::ssub_match sub_match = link_match[1];
    std::string piece = sub_match.str();
    //std::cout << piece << '\n';
    links[current_title].push_back(piece);
    //std::cout << current_title << " > " << piece << std::endl;
    section = link_match.suffix().str();
  }
}

void regex_test(){
  //std::regex link_regex("\\[\\[(.+)\\]\\]");

  std::string s ("this [[subject]] has a submarine as [[a]] subsequence");
  std::smatch m;
  std::regex e ("\\[\\[([^ ]*)\\]\\]");   // matches words beginning by "sub"

  std::cout << "The following matches and submatches were found:" << std::endl;

  while (std::regex_search (s,m,e)) {
    for (auto x:m) std::cout << x << "\n";
    std::cout << std::endl;
    s = m.suffix().str();
  }

}

#include <stdio.h>
#include <mpi.h>
#include <regex>
#include <iostream>
#include <string>
#include <map>

int g_world_size;
int g_my_rank;
std::string g_filename = "enwiki-20160305-pages-articles.xml";

void parse_file(MPI_File *infile);
void regex_test();


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
  //parse_file(&infile);

  if (g_my_rank == 0){
    regex_test();
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
  MPI_Offset offset = max_size * g_my_rank;

  printf("Rank %d ==> filesize: %lld ==> chunk size: %lld ==> start point: %lld\n", g_my_rank, filesize, max_size, offset);

  std::regex title_regex("<title>(.+)</title>");

  int chunk_size = 1000;
  char * chunk;
  std::string current_title;

  std::map<std::string, std::vector<std::string>> links;

  while (offset < max_size && links.size() < 100){
    chunk = (char *)malloc( (chunk_size + 1)*sizeof(char));
    MPI_File_read_at(*infile, offset, chunk, chunk_size, MPI_CHAR, MPI_STATUS_IGNORE);
    chunk[chunk_size] = '\0';
    //printf("read in: %s", chunk);
    std::string chunk_string(chunk);

    std::smatch title_match;
    if (std::regex_search(chunk_string, title_match, title_regex)){
      /*for (size_t i = 0; i < title_match.size(); ++i) {
          std::ssub_match sub_match = title_match[i];
          std::string piece = sub_match.str();
          std::cout << "  submatch " << i << ": " << piece << '\n';
      }*/
      current_title = title_match[1].str();
      links[current_title] = std::vector<std::string>();
      //std::cout << "new page:" << current_title << std::endl;
    }

    offset += chunk_size;
    free(chunk);
  }
  for (const auto &key : links) {
    std::cout << key.first << std::endl;
    for (const auto &val: key.second){
      std::cout << "  " << val << std::endl;
    }
  }

}

void regex_test(){
  std::regex title_regex("<title>(.+)</title>");
  std::smatch title_match;
  std::string chunk_string = "this came before <title>test title</title> this is after";
  if (std::regex_search(chunk_string, title_match, title_regex)){
    for (size_t i = 0; i < title_match.size(); ++i) {
        std::ssub_match sub_match = title_match[i];
        std::string piece = sub_match.str();
        std::cout << "  submatch " << i << ": " << piece << '\n';
        std::cout << "    prefix " << title_match.prefix() << std::endl;
        std::cout << "    suffix " << title_match.suffix() << std::endl;
    }
  }else{
    std::cout << "no match" << std::endl;
  }

}

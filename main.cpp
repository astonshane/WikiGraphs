#include <stdio.h>
#include <mpi.h>
#include <regex>
#include <iostream>
#include <string>

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
  parse_file(&infile);

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
  long long int chunk_size = filesize / g_world_size;

  //get the start point for this rank in the file
  MPI_Offset start_point = chunk_size * g_my_rank;

  printf("Rank %d ==> filesize: %lld ==> chunk size: %lld ==> start point: %lld\n", g_my_rank, filesize, chunk_size, start_point);
}

void regex_test(){
  std::string fnames[] = {"foo.txt", "bar.txt", "baz.dat", "zoidberg"};
  // Extraction of several sub-matches
  std::regex pieces_regex("([a-z]+)\\.([a-z]+)");
  std::smatch pieces_match;

  for (const auto &fname : fnames) {
      if (std::regex_match(fname, pieces_match, pieces_regex)) {
          std::cout << fname << '\n';
          for (size_t i = 0; i < pieces_match.size(); ++i) {
              std::ssub_match sub_match = pieces_match[i];
              std::string piece = sub_match.str();
              std::cout << "  submatch " << i << ": " << piece << '\n';
          }
      }
  }
}

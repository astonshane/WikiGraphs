#include <stdio.h>
#include <mpi.h>
#include <regex>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Get the number of processes
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Get the rank of the process
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Print off a hello world message
  //printf("Hello world from processor %s, rank %d"
  //     " out of %d processors\n",
  //     processor_name, world_rank, world_size);


  if (world_rank == 0){
    // Simple regular expression matching
   std::string fnames[] = {"foo.txt", "bar.txt", "baz.dat", "zoidberg"};
   std::regex txt_regex("[a-z]+\\.txt");

   for (const auto &fname : fnames) {
       std::cout << fname << ": " << std::regex_match(fname, txt_regex) << '\n';
   }

   // Extraction of a sub-match
   std::regex base_regex("([a-z]+)\\.txt");
   std::smatch base_match;

   for (const auto &fname : fnames) {
       if (std::regex_match(fname, base_match, base_regex)) {
           // The first sub_match is the whole string; the next
           // sub_match is the first parenthesized expression.
           if (base_match.size() == 2) {
               std::ssub_match base_sub_match = base_match[1];
               std::string base = base_sub_match.str();
               std::cout << fname << " has a base of " << base << '\n';
           }
       }
   }

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
  // Finalize the MPI environment.
  MPI_Finalize();

  return 0;
}

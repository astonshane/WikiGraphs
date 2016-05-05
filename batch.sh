
#!/bin/sh
#SBATCH --job-name=friendster_test
#SBATCH -t 01:00:00
#SBATCH -D /gpfs/u/barn-shared/PCP5/PCP5stns/WikiGraphs/
#
#SBATCH --mail-type=ALL
#SBATCH --mail-user=astonshane@gmail.com

# run with sbatch -N32 -t 60 --partition small -o output.txt run32.sh
# sbatch --partition debug --nodes 8 --time 15 ./run-hello.sh
# srun --ntasks 256 --overcommit -o hello.log /gpfs/u/home/SPNR/SPNRcaro/barn/mpi-hello.xl 

module load xl
# srun -O -N8 --ntasks-per-node 16 -n 128 --time 60 --partition debug ./project.xl
srun --ntasks 128 -o output.log ./project.xl

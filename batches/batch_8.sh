#!/bin/sh
#SBATCH --job-name=friendster_test
#SBATCH -t 01:00:00
#SBATCH -D /gpfs/u/home/PCP5/PCP5stns/barn-shared/WikiGraphs
#
#SBATCH --mail-type=ALL
#SBATCH --mail-user=astonshane@gmail.com

# run with sbatch -N32 -t 60 --partition small -o output.txt run32.sh

module load xl

srun -O -N8 --ntasks-per-node 16 -n 128 --time 60 --partition small ./project.xl

#!/bin/bash
#SBATCH -p physical
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --output=outputs_1n8c_Gather
module load Python/3.4.3-goolf-2015a
time mpirun -np 8 python GeoProcess_Gather.py
echo "1 node 8 cores"

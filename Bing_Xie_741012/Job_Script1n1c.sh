#!/bin/bash
#SBATCH -p physical
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=outputs_1n1c_Gather
module load Python/3.4.3-goolf-2015a
time mpirun -np 1 python GeoProcess_Gather.py
echo "1 node 1 core"

This is a self contained python module to run
parameter sweeps with lists of parameters,
files listing values, or range delta lists.  It keeps state in a local
sqlite3 database and can be restarted from the last checkpoint
only running commands that have not completed.  

Very simply users can use the --writeConfig to get an example config
to edit.  This is a .ini formatted file
```
usage: runner.py [-h] [--server] [--status] [--reset] [--writeConfig]
                 configFile

Parametric Sweeper

positional arguments:
  configFile     run a slurm parameter sweep from this configuraion file

optional arguments:
  -h, --help     show this help message and exit
  --server       run the server (not to be run by users but by SLURM
  --status       get the status of the current sweep
  --reset        reset parameter sweep
  --writeConfig  write a new config file
```

This is that example showing the list of parameters, how to load from a file, and how to do a range
It also has the SLURM information needed to manage the job

```
[sweep info]
command=runme.exe param1 files --timestep=timesteps testflag
param1=1,2,5               ; a simple list of parameters
files=<files.txt>          ; load from a file (one per line)
timesteps=(1:50:0.5)        ; a range here 1 to 50 incremented by .5
testFlags=,-beta,-alpha    ; just an example that leaving a blank parameter
                           ; just leaves it out on the command line

[system config]
num_nodes=2                ; number of nodes to use
num_tasks_per_node=10      ; tasks to be run concurently on a node
allocation=TACC-DIC        ; the allocation to be charged
job_time=01:00:00          ; the time the job should take
queue_name=normal          ; the queue to submit the job to
modules=python3,impi       ; the list of TACC modules to load
```

Advanced users can use the command variable with a comma sepparated list of commands.
These will run as a set of commands, so all of the first commands will complete before 
any of the second commands start making a simple DAG.  

The script when run to execute the sweep of parameters then simply creates the slurm job
script and submits it which then runs the same python script in `--server` mode which then
starts up the manager and manages tasks on the node or nodes available much like the parametric
parametric launcher does

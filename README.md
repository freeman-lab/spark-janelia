# Spark Janelia

A script for launching batch Spark cluster jobs under an LSF or SGE scheduler.

Earlier versions of this script supported a wider range of options but support for 
them was removed in late 2020 when Janelia's HPC was upgraded.  At that 
time, there was little interest in updating support for the other options.  

---
### Initial Setup
Clone this repository to a location that is accessible from the host(s) where 
you launch High Performance Compute (HPC) jobs:
```
cd ${HOME}
git clone https://github.com/JaneliaSciComp/spark-janelia
```
A python 3.* environment is needed to run the spark-janelia script, 
so make sure that is also available on the host(s) where you launch HPC jobs.

---
### Generate and Launch Run
              
You can generate a run (see details in next section) and launch it in one step like this: 
```bash
spark-janelia <args> generate-and-launch-run --submitargs <submit_arguments>

# same thing using legacy 'lsd' task name for compatibility with old flintstone scripts
spark-janelia <args> lsd --submitargs <submit_arguments>   
```

---
### Generate Run

The spark-janelia script generates a "run": a set of parameterized scripts that 
use your HPC scheduler to start up a Spark cluster, run a driver application on it, 
and then shut everything down.

By default, the generated run scripts are organized as follows:
```bash
${HOME}/.spark             # --run_parent_dir default (can be changed on command line) 
    /<YYYYmmdd_HHMMSS>     # timestamp of run generation
        /conf              # spark configuration files for run
        /logs              # log files for run
        /scripts           # parameterized scripts for run
```

To generate a run without launching it:
```bash
spark-janelia <args> generate-run --submitargs <submit_arguments>
```

#### Legacy Arguments
``` 
--driverslots|-d  {1-48}                 # override default number of slots (32) for driver
--hard_runtime|-t {hours:minutes}        # override default hard runtime of 8 hours 
--lsf_project|-P  {project name}         # sets project to be billed if not users's own group
--minworkers                             # override minimum number of running workers (1) before driver launches
--nnodes|-n       {#}                    # override maximum number of workers (2)
--submitargs|-s   {args ...}             # arguments to pass to spark-submit when launching driver 
--version|-v      {2.3.1|3.0.1|current}  # override default version of Spark (3.0.1) to use
                                         #   ls -d /misc/local/spark-* 
```

#### New Arguments
``` 
--common_job_args  {args}           # additional arguments for all HPC job submissions (e.g. -q test) 
--consolidate_logs                  # overrides --run_worker_dir and writes worker logs to central run logs directory 
--gb_per_slot      {#}              # override default (15GB) memory allocated per slot
--hpc_type         {lsf|sge}        # override default HPC type of lsf
--java_home        {path}           # override environment ${JAVA_HOME}
--run_local_dirs   {path,...,path}  # override default storage directories used on worker nodes for shuffle and RDD data                                     
--run_parent_dir   {path}           # override default parent directory for run scripts and logs 
--run_worker_dir   {path}           # override default directory on worker nodes containing worker jar and log files 
--spark_home       {path}           # override environment ${SPARK_HOME} and --version with this explicit path 
--worker_opts      {args}           # additional arguments to pass to spark-class when launching workers 
                                    #   e.g. -Dspark.worker.cleanup.enabled=true (to remove SPARK_WORKER_DIR on exit) 
--worker_slots     {#}              # override default numbner of slots (32) for each worker 
```

Note: Janelia cluster resource information is listed at 
<http://wiki.int.janelia.org/wiki/display/ScientificComputing/Janelia+Compute+Cluster>

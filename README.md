# Spark Janelia

A script for launching and controlling Spark clusters under a scheduler on an HPC cluster. 

spark-janelia-lsf is an updated version that works with IBM Spectrum LSF, which will become our new scheduler in the coming months. 

---
### Using Spark
These scripts automate many of the tasks required to start and manage a Spark job on an HPC cluster scheduled with LSF. 

#### Initial setup
Navigate to a location in your home directory where you will store these scripts, then call
```
git clone https://github.com/JaneliaSciComp/spark-janelia
```
Add a line with this location to your bash profile (usually located in `~/.bash_profile`)
```
export PATH=/path/to/spark-janelia:$PATH
```
While doing this, also add this line to point your PATH to a recent version of Python (optional, but it will make things easier):
```
export PATH=/usr/local/python-2.7.11/bin/:$PATH
```
If you want to continue in the same session, source your profile with `source ~/.bash_profile`.

#### Additional setup for sparkbatch (tightly coupled) usage:

SSH in to a login node.

```
ssh login2
```
If you have never done so, set up keyless SSH
```
ssh-keygen -t dsa
```
Accept all of the default settings by hitting "return" three times. Then continue with
```
cd .ssh
cat id_dsa.pub >> authorized_keys
```

---
### Quick start to Jupyter Notebook

While logged into a login node, submit the following to launch a cluster, then start a Jupyter Notebook server: 
```
spark-janelia-lsf launch-notebook -n <number_of_nodes>
```

The script will return a URL to the notebook, which you can access on your computer (please don't run a browser on the login node)

To terminate this cluster (including the notebook server), on the login node, run
```
spark-janelia-lsf stopcluster
```

---
### Details and more complex use cases
While logged in to a login nodes, submit a request to run a group of nodes as a Spark cluster using:
```
spark-janelia-lsf launch -n <number_of_workers>
```

This will start a master, then <number_of_nodes> workers. Each worker will run as a separate job, which means that the cluster will be available with as many nodes out of the requested number that can be scheduled immediately. Further nodes will come online as slots become available. 

Each node has 15 cores and 75GB of RAM for Spark to use. You should target the number of nodes you request based on the size of the data you are working with or the amount of neccessary computation (or both). A good rule of thumb is to use enough nodes so the total amount of RAM is about twice the total size of your data set. Check the status of your request using the `qstat` command. When the status is listed as `r` (for "ready"), proceed.

```
spark-janelia-lsf launchall -n <number_of_workers>
```
This will launch a cluster in the tightly coupled method, where all nodes will need to be available in order fo the cluster to launch. This requires an ssh key (see above).

In either case, if you do not specify -n <number of workers>, the  cluster will launch with one master node and 2 worker nodes. 

Every Spark cluster has a unique node designated as the "driver" from which all computations are initiated. To start and log in to the driver for your Spark cluster, use:

```
spark-janelia-lsf login
```
You can now run Spark applications as described below.

#### SHORTCUT: Running a Spark cluster in interactive mode (no notebook):

```
spark-janelia-lsf <args> launch-in
```
This will launch the spark cluster, then start an interactive job on a driver node. Note that you can specify -d <num_slots> for number of slots to use on the driver. This is limited to whole nodes; ie 16 or 32 slots. 

#### Various options for interactive jobs on the Spark cluster
To start the Spark interactive shell in iPython call
```
spark-janelia-lsf start
```
This will start a shell with Spark running and its modules available.

To start the Spark interactive shell in Scala call
```
spark-janelia-lsf start-scala
```
---
### To submit a Spark application to run
```
spark-janelia-lsf submit -s <submit_arguments>
```
Where `submit_arguments` is a string of arguments you would normally pass to `spark-submit`, as described in the [Spark documentation](https://spark.apache.org/docs/1.2.0/submitting-applications.html).

submit will now submit a separate job to a driver node rather than sshing to the master node and potentially overrunning the available resources. 

#### Running a Spark application with automatic shut down of the Spark cluster after completion
If you want to run an application with unknown runtime
it will be helpful to have the Spark cluster shut down automatically after completion of the application.

```bash
spark-janelia-lsf <args> lsd -s <submit_arguments>
```
will submit your application and delete the cluter job, once it has finished. `lsd` is short for `launch-submit-and-destroy` and the meaning of `submit_arguments` is along the lines of `spark-janelia-lsf submit`. For an example of how to set up a Python script for this workflow, see the example in `examples/lsd-example.py`.



---
### Changing cluster size (not available for sparkbatch/launchall type jobs)

#### Adding nodes to a running cluster
To add nodes to your Spark cluster:
```
spark-janelia-lsf -n <NUM_NODES> -j <JOBIDofMASTER> add-workers
```

#### Removing workers from a running cluster
To remove nodes from a Spark cluster:
```
spark-janelia-lsf -j <JOBIDofMASTER> -n <NUM_NODES> [-f, --force] remove-workers
```
By default the command will present a list of nodes to choose from. With the `-f` flag, it will remove the number of nodes requested arbitrarily. 

---
### Using the Jupyter notebook
When running Spark in Python, the Jupyter notebook is a great way to run analyses and inspect and visualize results. This requires no extra setup, simple add the `-b` flag when starting Python: 

Now, when starting Spark, add the `-b` flag, as in:
```
spark-janelia-lsf start -b
```
Instead of opening a shell, this will launch a Jupyter Notebook server on the driver. You will see a website address printed next to `View your notebooks at...`. Go to this address in a web browser. Your browser will complain that the connection is not trusted; just ignore and proceed.

When prompted, enter the password you chose above. You should now have a graphical interface to the directory from which you launched the IPython Notebook server. From here you can create a new notebook or edit a previously-existing one.

To shut down the IPython Notebook server, return to the terminal where the server is running and type `Ctrl+C+C`.

NOTE: You can't run Spark jobs in multiple notebooks at once. If you start one notebook, then start another, nothing will execute in the second one until you shutdown the first one (by clicking the shutdown button in the notebook browser).

If you would like to use the Jupyter notebook for non-Spark related work, we also provide a script for configuring the notebook for general use on the cluster. As a one time operation, call this script
```
setup-notebook-janelia
```

This will configure your Jupyer Notebook settings to launch a notebook server. Then, from any node, you can start a server with
```
jupyter notebook
```

---
### Shutting down
When you are finished with all of your jobs, log out of the driver with the simple command `exit` in the command line. Finally, release your nodes with
```
spark-janelia-lsf stopcluster
```

If you have multiple clusters running, this will present you with a list of clusters to shut down. You can also specify -j <jobid_of_master> to shut down a particular cluster. 

DEPRECATED: 
```
spark-janelia-lsf destroy -j <jobid_of_master>
```

You can check that these nodes have successfully been released with the `bjobs` command.

---
### Other options
``` 
-v|--version {current|test|2|rc}  #Sets version of Spark to use. See the wiki for information about the available versions. 
-f|--force                        #Skips selection list and/or confirmation of action
-d|--driverslots {16|32}          #Sets number of slots to use on a driver job. Only available in whole node increments (16 or 32 slots)
-i False                          #Unsets ipython as the shell for pyspark
-t|--hard_runtime {minutes}         #Overrides default hard runtime of 8 hours. In LSF this is set in minutes. At the end of this time, the jobs will exit automatically. 
-o|--driveroutfile {/path/to/output}  #Adds output file for submission. If set to /dev/null, will prevent email from being sent on Driver close.
-P|--project {project name}       #Sets project to be billed if not users's own group/project.
update                            #Downloads the latest version from github
```
NOTE: sleep_time has been changed to hard_runtime to more accurately reflect the purpose of this option. 
---
## Questions and comments
Please submit a ticket to the HHMI Helpdesk if you run into any issues with this script or Spark clusters. 

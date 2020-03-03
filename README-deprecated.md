# Spark Janelia

Scripts, utilities, and community for launching Spark on the Janelia Research Center cluster. If you have questions you can come hang out in the public freeman lab [chatroom](https://gitter.im/freeman-lab/discussion) on gitter (or #thefreemanlab on freenode). 

spark-janelia-lsf is an updated version that works with IBM Spectrum LSF, which will become our new scheduler in the coming months. 

---
### Using Spark
These scripts automate many of the tasks required to start and manage a Spark job.

#### Initial setup
SSH in to one of Janelia's cluster login nodes (see [Scientific Computing](http://wiki.int.janelia.org/wiki/display/ScientificComputing/Janelia+Compute+Cluster) for more information).
```
ssh login2.int.janelia.org
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
Navigate to a location in your home directory where you will store these scripts, then call
```
git clone https://github.com/freeman-lab/spark-janelia
```
Add a line with this location to your bash profile (usually located in `~/.bash_profile`)
```
export PATH=/path/to/spark-janelia:$PATH
```
While doing this, also add this line to point your PATH to a recent version of Python:
```
export PATH=/usr/local/python-2.7.11/bin/:$PATH
```
If you want to continue in the same session, source your profile with `source ~/.bash_profile`.

#### Starting a cluster
While logged in to one of Janelia cluster's login nodes, submit a request to run a group of nodes as a Spark cluster using:
```
spark-janelia launch -n <number_of_nodes>
```
This will start a master, then <number_of_nodes> workers. Each worker will run as a separate job, which means that the cluster will be available with as many nodes out of the requested number that can be scheduled immediately. Further nodes will come online as slots become available. 

Each node has 15 cores and 75GB of RAM for Spark to use. You should target the number of nodes you request based on the size of the data you are working with or the amount of neccessary computation (or both). A good rule of thumb is to use enough nodes so the total amount of RAM is about twice the total size of your data set. Check the status of your request using the `qstat` command. When the status is listed as `r` (for "ready"), proceed.

```
spark-janelia launchall -n <number_of_nodes>
```
This will launch a cluster in the old method, where all nodes will need to be available in order fo the cluster to launch. 

Every Spark cluster has a unique node designated as the "driver" from which all computations are initiated. To log in to the driver for your Spark cluster, use:
```
spark-janelia login
```
You can now run Spark applications as described below.

#### Running basic jobs
To start the Spark interactive shell in Python call
```
spark-janelia start
```
This will start a shell with Spark running and its modules available.

To start the Spark interactive shell in Scala call
```
spark-janelia start-scala
```
And to submit a Spark application to run call
```
spark-janelia submit -s <submit_arguments>
```
Where `submit_arguments` is a string of arguments you would normally pass to `spark-submit`, as described in the [Spark documentation](https://spark.apache.org/docs/1.2.0/submitting-applications.html).

#### Running a Spark application with automatic shut down of the Spark cluster after completion
If you want to run an application with unknown runtime
it will be helpful to have the Spark cluster shut down automatically after completion of the application.
```bash
spark-janelia <args> lsd -s <submit_arguments>
```
will submit your application and delete the cluter job, once it has finished. `lsd` is short for `launch-submit-and-destroy` and the meaning of `submit_arguments` is along the lines of `spark-janelia submit`. For an example of how to set up a Python script for this workflow, see the example in `examples/lsd-example.py`.

#### Adding nodes to a running cluster
To add nodes to your Spark cluster:
```
spark-janelia -n <NUM_NODES> -j <JOBIDofMASTER> add-workers
```

#### Removing workers from a running cluster
To remove nodes from a Spark cluster:
```
spark-janelia -j <JOBIDofMASTER> -n <NUM_NODES> [-f, --force] remove-workers
```
By default the command will present a list of nodes to choose from. With the `-f` flag, it will remove the number of nodes requested arbitrarily. 

---
### Using the Jupyter notebook
When running Spark in Python, the Jupyter notebook is a great way to run analyses and inspect and visualize results. This requires no extra setup, simple add the `-b` flag when starting Python: 

Now, when starting Spark, add the `-b` flag, as in:
```
spark-janelia start -b
```
Instead of opening a shell, this will launch a Jupyter Notebook server on the driver. You will see a website address printed next to `View your notebooks at...`. Go to this address in a web browser. Your browser will complain that the connection is not trusted; just ignore and proceed.

When prompted, enter the password you chose above. You should now have a graphical interface to the directory from which you launched the IPython Notebook server. From here you can create a new notebook or edit a previously-existing one.

To shut down the IPython Notebook server, return to the terminal where the server is running and type `Ctrl+C+C`.

NOTE: You can't run Spark jobs in multiple notebooks at once. If you start one notebook, then start another, nothing will execute in the second one until you shutdown the first one (by clicking the shutdown button in the notebook browser).

If you would like to use the Jupyter notebook for non-Spark related work, we also provide a script for configuring the notebook for general use on the cluster. As a one time operation, call this script
```
setup-notebook-janelia
```

This will configure your Jupyer Notebook settings to be compatible with Janelia's cluster. Then, from any node, you can start a server with
```
jupyter notebook
```

---
### Shutting down
When you are finished with all of your jobs, log out of the driver with the simple command `exit` in the command line. Finally, release your nodes with
```
spark-janelia destroy
```
You can check that these nodes have successfully been released with the `qstat` command.

For multiple running clusters, use
```
spark-janelia stopcluster
```
with an optional -j jobid flag. 
---
## Questions and comments
Many Spark users at Janelia hang out in the freeman lab [gitter chat rooom](https://gitter.im/freeman-lab/discussion). If you have questions or comments, or just want to join the conversation, please drop by!

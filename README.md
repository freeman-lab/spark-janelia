# Spark Janelia

Shell scripts and utilities for launching Spark and running applications (e.g. Thunder) on the Janelia Research Center cluster. We maintain a chatroom on gitter if you have questions about usage or running in this environment.

[![Join the chat at https://gitter.im/freeman-lab/spark-janelia](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/freeman-lab/spark-janelia?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

---

### Getting up and running with Thunder at Janelia

#### Initial setup
1. Log in to one of Janelia's login nodes (see Janelia's [Scientific Computing](http://wiki.int.janelia.org/wiki/display/ScientificComputing/Janelia+Compute+Cluster) resources for information on how to do this).
2. Choose a location in your home directory where you will store the scripts from this repository. From this location, download the scripts with `git clone https://github.com/freeman-lab/spark-janelia`.
3. Inside of this folder you will find multple scripts. To download the files for Thunder, run the `thunder-janelia` script with `thunder-janelia install`. This will create a new folder at the top level of your home directory and download the files for Thunder into that folder. If you would like to choose a different location, use the `-p` flag: `thunder-janelia install -p path_to_thunder`. 
4. OPTIONAL: If you would like to use the IPython Notebook interface for working with Thunder, then now is a good time to also run the `notebook-janelia` script. This will configure your IPython Notebook settings to be compatible with Janelia's cluster. The only thing you need to do is provide a password when prompted. This password will allow you to make a secure connection to the IPython Notebook server.

#### Launching a Spark cluster and starting Thunder
1. While logged in to one of Janelia's login nodes, use the `spark-janelia` script to submit a request for a group of nodes configured to run as a Spark cluster: `spark-janelia launch -n number_of_nodes`. Note: each node contains multiple cores and a large chunk of RAM, so 5 to 10 nodes is a reasonable number for many analyses. Check the status of your request using the `qstat` command. When the status is listed as `r` (for "ready"), you are ready for the next step.
2. Every Spark cluster has unique node designated as the "driver" from which all computations are initiated. To log in to the driver for your Spark cluster, use: `spark-janelia login`.
3. The driver will also have your home directory mounted, so the scripts will be in the same relative location. Now on the driver, launch Thunder with `thunder-janelia start`. If you installed Thunder to a location other than the default during the initial setup, then you will need the specify where it was installed: `thunder-janelia start -p path_to_thunder`. This will open the IPython terminal with the modules for [Spark](https://spark.apache.org/) and [Thunder](http://thefreemanlab.com/thunder/) preloaded. See their respective project pages for information on how to use these tools in your anlyses. OPTIONAL: If you would rather use the IPython Notebook (instead of the terminal), see the instructions in the next section.
4. When you are finished and have exited out of IPython (terminal or Notebook), log out of the driver with the simple command `exit` from the command line. Finally, release your nodes with `spark-janelia destroy`. You can check that these nodes have successfully been released with the `qstat` command.

#### OPTIONAL: Using IPython Notebook
1. Follow all of the instructions above for setting up Thunder and launching a Spark cluster. Be sure to not miss the OPTIONAL step in the setup, as it is necessary for using IPython Notebook.
2. Once you are logged in to the driver, start Thunder with an extra `-i` flag to signal that IPython Notebook should be used: `thunder-janelia -i`. Instead of opening the IPython terminal, this will launch an IPython Notebook server on the driver.
3. Note the hostname of your driver. This can be done by running the `qstat` command or by inspecting the command prompt. It should have the form `hXXuYY.int.janelia.org`, where `XX` and `YY` are two-digit numbers.
4. Open a web browers and enter the URL `https://hXXuYY.int.janelia.org:9999`. Your browser is likely to complain that the connection is not trusted, as we are using secure-http but do not have a legitimately signed cert. This can be bypassed by simply ignoring the warning.
5. When prompted, enter the password you chose in the OPTIONAL step of the Thunder setup. You should now have a graphical interface to the directory from which you launched the IPython Notebook server. From here you can create a new notebook or edit a perviously-existing one.
6. To shut down the IPython Notebook server, return to the terminal where the server is running and enter `Ctrl+C+C`.

---

## Questions and comments
Many of us who regularly use Thunder and Spark here at Janelia can often be found in the [Gitter chat rooom]() for this repository. If you have any questions or comments, or just want to join the conversation, please feel free to drop in!

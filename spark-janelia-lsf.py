#!/usr/bin/env python

import os
import glob
import sys
import argparse
import subprocess
import time
import multiprocessing
import traceback
from distutils import spawn

#########Variables to change for cluster environment
def_node_slots = 16
def_options = "-R \"sandy\""
alt_slots = 32
alt_options = ""
queue = 'spark'
SPARKVERSIONS = {   
    'current': '/misc/local/spark-current/',
    'rc': '/misc/local/spark-rc/',
    'test': '/misc/local/spark-test',
    '2': '/misc/local/spark-2'
}                   
defaultversion = 'current'
####################################################

def getmasterbyjobID(jobID):
    masterhost = None
    bjobsout = subprocess.check_output("bjobs -Xr -noheader -J master -o \"JOBID EXEC_HOST\" 2> /dev/null", shell=True)
    masters = bjobsout.splitlines()
    for master in masters:
        if str(jobID) in master.split()[0]:
            masterhost = master.split('*')[1]
    return masterhost

def getallmasters():
    masters = []
    command = "bjobs -X -noheader -J master -o \"JOBID STAT EXEC_HOST\" 2> /dev/null"
    bjobsout = subprocess.check_output(command, shell=True)
    if not bjobsout: 
        print "No masters found. Please specify job ID for sparkbatch jobs."
        sys.exit()
    for outline in bjobsout.splitlines():
        outline = outline.split()
        masterdict = {'jobid':outline[0], 'status':outline[1], 'host':outline[2].split('*')[1]}
        masters.append(masterdict)
    return masters

def getworkersbymasterID(masterID):
    workers = []
    command = "bjobs -X -noheader -J W{} -o \"JOBID STAT EXEC_HOST\" 2> /dev/null".format(masterID)
    bjobsout = subprocess.check_output(command, shell=True)
    for outline in bjobsout.splitlines():
        outline = outline.split()
        workerdict = {'jobid':outline[0], 'status':outline[1], 'host':outline[2].split('*')[1]}
        workers.append(workerdict)
    return workers

def getdriversbymasterID(masterID):
    drivers = []
    command = "bjobs -X -noheader -J D{} -o \"JOBID STAT EXEC_HOST\" 2> /dev/null".format(masterID)
    bjobsout = subprocess.check_output(command, shell=True)
    for outline in bjobsout.splitlines():
        outline = outline.split()
        driverdict = {'jobid':outline[0], 'status':outline[1], 'host':outline[2].split('*')[1]}
        drivers.append(driverdict)
    return drivers

def launchall(runtime):
    sparktype = args.version
    slots = def_node_slots + args.nnodes*def_node_slots
    if runtime == "None":
        options = "-n {}".format(slots)
    else:
        options = "-n {} -W {}".format(slots,runtime)
    #bsub requires argument for command, but the esub replaces it automatically
    output = subprocess.check_output(["bsub -a \"sparkbatch({})\" -J sparkbatch {} commandstring".format(sparktype,options)], shell=True)
    print 'Spark job submitted with {} workers ({} slots)'.format(args.nnodes, slots)
    jobID = output[1].lstrip("<").rstrip(">")
    return jobID

def launch(runtime):
    if not os.path.exists(os.path.expanduser('~/sparklogs')):
        os.mkdir(os.path.expanduser('~/sparklogs'))
    
    sparktype = args.version
    masterjobID = startmaster(sparktype, runtime)
    time.sleep(10)
    try:
        for i in range(args.nnodes):
            startworker(sparktype, masterjobID, runtime)
    except:
        print "Worker launch failed"
        #traceback.print_exc()
        sys.exit(1)
    return masterjobID

def startmaster(sparktype, runtime):
    options = None
    if runtime is not None:
        options = "-W {}".format(runtime)
    #bsub requires argument for command, but the esub replaces it automatically
    process = subprocess.Popen(["bsub -a \"spark(master,{})\" {} commandstring".format(sparktype,options)], shell=True, stdout=subprocess.PIPE)
    rawout = process.communicate()
    try:
        masterjobID = rawout[0].split(" ")[1].lstrip("<").rstrip(">")
    except:
        sys.exit(1)
    print "Master submitted. Job ID is {}".format(masterjobID)
    return masterjobID

def startworker(sparktype, masterjobID, runtime):
    masterURL = None
    masterURL = getmasterbyjobID(masterjobID)
#insert logic to deal with pending here
    while masterURL is None:
        masterURL = getmasterbyjobID(masterjobID)
        if masterURL is None: 
            waitformaster = raw_input("No master with the job id {} running. Do you want to wait for it to start? (y/n) ".format(masterjobID))
            if waitformaster == 'n':
                print "Master may be orphaned. Please check your submitted jobs."
                sys.exit(0)
            else:
                time.sleep(60)

    if runtime is not None:
        command = "bsub -a \"spark(worker,{})\" -J W{} -W {} commandstring".format(sparktype,masterjobID,runtime)
    else:
        command = "bsub -a \"spark(worker,{})\" -J W{} commandstring".format(sparktype,masterjobID,runtime)
    os.system(command)

def getenvironment():
    #Set MASTER
    if "MASTER" not in os.environ:
        masterlist = getallmasters()
        masterjobID = selectionlist(masterlist,'master')
        masterhost = getmasterbyjobID(masterjobID)
        os.environ["MASTER"] = str("spark://{}:7077".format(masterhost))
    else:
        masterhost = os.getenv('MASTER').lstrip("spark://").replace(":7077","")

    #Set SPARK_HOME
    if "SPARK_HOME" not in os.environ:
        versout = subprocess.check_output("bjobs -noheader -o 'COMMAND' -r -m {}".format(masterhost), shell=True)
        verspath = SPARKVERSIONS[versout.split()[1]]
        os.environ["SPARK_HOME"] = str(verspath) 

    #Set PATH
    if args.version not in os.environ['PATH']:
        os.environ["PATH"] = str("{}/bin:{}".format(args.version, os.environ['PATH']))

def checkslots(nodeslots=def_node_slots):
    if nodeslots == def_node_slots:
        options = "{} {}".format(def_node_slots, def_options)
    elif nodeslots == alt_slots:
        options = "{} {}".format(alt_slots, alt_options)
    else: 
        print "You must request an entire node for a Driver job. Please request {} or {} slots.".format(def_node_slots, alt_slots)
        sys.exit()
    return options

def login(nodeslots):
    getenvironment()
    options = checkslots(nodeslots)
    command = "bsub -Is -n {} /bin/bash".format(options)
    os.system(command)
    
def submit(jobID, nodeslots, sparkcommand):
    getenvironment()
    options = checkslots(nodeslots)
    if args.sleep_time is None:
        runtime = "8:00"
    else:
        runtime = args.sleep_time
    command = "bsub -W {} -n {} -J D{} \"{}\"".format(runtime, options, jobID, sparkcommand) 
    rawout = subprocess.check_output(command, shell=True)
    driverJobID = rawout.split(" ")[1].lstrip("<").rstrip(">")
    return driverJobID

def destroy(jobID):
    if jobID is '':
        print "Please specify a job ID for a master or cluster to tear it down."
        sys.exit()
    else:
        bkilljob(jobID)
        workers = []
        workers = getworkersbymasterID(jobID)
        if workers:
            for worker in workers:
                bkilljob(worker['jobid'])
        drivers = []
        drivers = getdriversbymasterID(jobID)
        if drivers:
            for driver in drivers:
                bkilljob(driver['jobid'])

def start(command = 'pyspark'):
    if args.ipython is True:
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'ipython'
    if args.notebook is True:
        address = setupnotebook()
        print('\n')
        print('View your notebooks at http://' + os.environ['HOSTNAME'] + ':9999')
        print('View the status of your cluster at http://' + address + ':8080')
        print('\n')
    os.system(command)

def setupnotebook():
    getenvironment()
    if spawn.find_executable('jupyter') is None:
        print "Jupyter not found. Wrong python in $PATH?"
        sys.exit(1)
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
    os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook --ip "*" --port 9999 --no-browser'
    address = os.getenv('MASTER')[8:][:-5]
    return address 
    
def launchAndWait():
        jobID  = launch(args.sleep_time)
        master = ''     
        while( master == '' ):
            master = getmasterbyjobID(jobID)
            time.sleep(10) # wait 30 seconds to avoid spamming the cluster
            sys.stdout.write('.')
            sys.stdout.flush()
        return master, jobID

def submitAndDestroy(jobID, driverslots, sparksubargs):
    master=getmasterbyjobID(jobID)
    os.environ["MASTER"] = "spark://{}:7077".format(master)
    driverjobID = submit(jobID, driverslots, sparksubargs)
    drivercomplete = False
    while not drivercomplete:
        driverstat = subprocess.check_output("bjobs -noheader -o 'STAT' {}".format(driverjobID), shell=True)
        if driverstat is "EXIT" or "DONE":
            drivercomplete = True
        time.sleep(30)
    destroy(jobID)

def checkforupdate():
    currentdir = os.getcwd()
    scriptdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptdir)
    output = subprocess.check_output('git fetch --dry-run 2>&1', shell=True) 
    if "origin" in output: 
        reply = raw_input("This script is not up to date. Would you like to update now? (y/n) ")
        if reply == 'y':
            update()
            sys.exit()
        else: 
            return

def update():
    currentdir = os.getcwd()
    scriptdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptdir)
    try:
        os.system('git pull origin master')
        os.system('git pull')
        print "Update successful."
    except:
        print "Update failed."
    os.chdir(currentdir)

def stopworker(masterjobID, terminatew, workerlist, skipcheckstop):
    workername = "W{}".format(masterjobID)
    jobtokill = ""
    statuses = {}
    for worker in workerlist:
        statuses[worker['jobid']] = worker['status']
    if "PEND" in statuses.values() and not skipcheckstop:
        terminatew = raw_input("Terminate waiting job(s) first? (y/n) ")
    for wjobID in statuses.keys():
        if statuses[wjobID] == 'PEND' and terminatew == 'y':
            jobtokill = wjobID
            break
        elif statuses[wjobID] == 'RUN' and not skipcheckstop:
            jobtokill = selectionlist(workerlist, 'worker')
            break
        else:
            jobtokill = wjobID
            break
    try:
        if jobtokill != "":
            bkilljob(jobtokill)
            time.sleep(3)
    except:
        print "No running or waiting workers found corresponding to Master job {}".format(masterjobID)
        traceback.print_exc()
        sys.exit()
    return terminatew


def bkilljob(jobID):
    command = "bkill {}".format(jobID)
    os.system(command)


def checkstop(inval, jobtype):
    if jobtype == "master":
        check = raw_input("Stop master with job id {} (y/n):".format(inval))
    else:
        check = raw_input("Remove {} workers? (y/n):".format(inval))
    if check != "y":
        print "Operation cancelled."
        sys.exit()
    else:
        return True

def selectionlist(joblist, jobtype):
    i = 0 
    selectlist = {}
    if len(joblist) == 1: 
        jobID = joblist[0]['jobid']
        return jobID
    else:
        print "Select {} from list below:".format(jobtype)
        for job in joblist:
            i = i + 1 
            selectlist[i] = job['jobid']
            print "{}) Host: {} jobID: {} Status: {}".format(i, job['host'], job['jobid'], job['status'])
        while True:
            selection = int(raw_input("Selection? "))
            if selection <= i:
                jobID = selectlist[selection]
                skipcheckstop = True
                break
            else:
                print "Invalid selection."
        return jobID

if __name__ == "__main__":

    checkforupdate()
    versiontypes = SPARKVERSIONS.keys()

    skipcheckstop = False
    parser = argparse.ArgumentParser(description="launch and manage spark cluster jobs")
                        
    choices = ('launch', 'launchall', 'login', 'destroy', 'start', 'start-scala', 'submit', 'lsd', 'launch-in', 'launch-notebook', 'update', 'add-workers', 'remove-workers', 'stopcluster')
                        
    parser.add_argument("task", choices=choices)
    parser.add_argument("-n", "--nnodes", type=int, default=2, required=False)
    parser.add_argument("-i", "--ipython", action="store_true")
    parser.add_argument("-b", "--notebook", action="store_true")
    parser.add_argument("-v", "--version", choices=versiontypes, default=defaultversion, required=False)
    parser.add_argument("-j", "--jobID", type=int, default=None, required=False)
    parser.add_argument("-t", "--sleep_time", type=int, default=None, required=False)
    parser.add_argument("-s", "--submitargs", type=str, default='', required=False)
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("-d", "--driverslots", type=int, default=def_node_slots, required=False)
                        
    args = parser.parse_args()
                        
    if args.force == True:
        skipcheckstop = True

    if args.task == 'launch':
        masterjobID = launch(args.sleep_time)
        masterurl = "spark://{}:7077".format(getmasterbyjobID(masterjobID))
        print "To set $MASTER environment variable, enter export MASTER={} at the command line.".format(masterurl)
                        
    if args.task == 'launchall':
        masterjobID = launchall(str(args.sleep_time))

    elif args.task == 'login':
        login(int(args.driverslots))         
                        
    elif args.task == 'destroy':
        destroy(args.jobID or '')
                        
    elif args.task == 'start':
        start()         
                        
    elif args.task == 'start-scala':
        start('spark-shell') 
                        
    elif args.task == 'submit':
        sparksubargs = 'spark-submit {}'.format(args.submitargs)
        submit(args.jobID, int(args.driverslots), sparksubargs)
                        
    elif args.task == 'lsd':
        master, jobID = launchAndWait()
        master = 'spark://%s:7077' % master
        sparksubargs = 'spark-submit {}'.format(args.submitargs)
        print('\n')     
        print('%-20s%s\n%-20s%s' % ( 'job id:', jobID, 'spark master:', master ) )
        print('\n')     
        p = multiprocessing.Process(target=submitAndDestroy, args=(jobID, args.driverslots, sparksubargs))
        p.start()       

    elif args.task == 'launch-in':
        master, jobID = launchAndWait()
        print '\n\nspark master: {}\n'.format(master)
        login(int(args.driverslots))

    elif args.task == 'launch-notebook':
        master, jobID = launchAndWait()
        #print '\n\nspark master: {}\n'.format(master)
        os.environ['MASTER'] = "spark://{}:7077".format(master)
        address = setupnotebook()
        driverjobID = submit(jobID, int(args.driverslots), "pyspark")
        print driverjobID
        time.sleep(10)
        driverhost = subprocess.check_output("bjobs -X -noheader -o \"EXEC_HOST\" {}".format(driverjobID), shell=True)
        print driverhost
        print "Jupyter notebook at http://{}:9999".format(driverhost[3:].replace('\n',''))

    elif args.task == 'update':
        update()

    elif args.task == 'add-workers':
        if args.jobID is None:
            masterlist = getallmasters()
            masterjobID = selectionlist(masterlist,'master')
        else:
            masterjobID = args.jobID
        for node in range(args.nnodes):
            startworker(args.version, masterjobID, args.sleep_time)

    elif args.task == 'remove-workers':
        if args.jobID is None:
            masterlist = getallmasters()
            masterjobID = selectionlist(masterlist,'master')
        else:
            masterjobID = args.jobID
        terminatew = ""
        jobtype = "worker"
        for node in range(args.nnodes):
            workerlist = getworkersbymasterID(str(masterjobID))
            terminatew = stopworker(str(masterjobID), terminatew, workerlist, skipcheckstop)
   
    elif args.task == 'stopcluster':
        if args.jobID is not None:
            masterjobID = args.jobID
        else: 
            masterlist = getallmasters()
            masterjobID = selectionlist(masterlist,'master')
        if not skipcheckstop:
            checkstop(masterjobID, 'master')
        destroy(masterjobID)

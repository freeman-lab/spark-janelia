#!/misc/local/python-2.7.11/bin/python2.7

import os
import glob
import sys
import argparse
import subprocess
import time
import multiprocessing
import traceback
import xmltodict

#def findMasters():
#    return [job['queue_name'].split('@')[1].split('.')[0] for job in getqstat() if job['JB_name']=='master']
#
#def findmaster():
#    rawout = getqstat()
#    jobID = "" 
#    masterlist = {}
#    for i in range(len(rawout)):
#        unpack = rawout[i]
#        jobID = str(unpack[u'JB_job_number'])
#        if "sparkflex" in str(unpack[u'jclass_name']) and "master" in str(unpack[u'JB_name']):
#            masterlist[jobID] = (str(unpack[u'queue_name']).replace('hadoop2@',''),str(unpack[u'@state']))
#        elif "spark.default" or "spark-2" or "spark-rc" in str(unpack[u'jclass_name']):
#            masterlist[jobID] = (str(unpack[u'queue_name']).replace('hadoop2@',''),str(unpack[u'@state']))
#    if len(masterlist.keys()) != 0:
#        return masterlist
#    else:   
#        print "No masters found. If deleting workers alone, please use Master's Job ID."

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
        masterdict = {'jobid':outline[0], 'status':outline[1], 'host':outline[2].lstrip("16*")}
        masters.append(masterdict)
    return masters

def getworkersbymasterID(masterID):
    workers = []
    command = "bjobs -X -noheader -J W{} -o \"JOBID STAT EXEC_HOST\" 2> /dev/null".format(masterID)
    bjobsout = subprocess.check_output(command, shell=True)
    for outline in bjobsout.splitlines():
        outline = outline.split()
        workerdict = {'jobid':outline[0], 'status':outline[1], 'host':outline[2].lstrip("16*")}
        workers.append(workerdict)

   # print workers
    return workers

def getqstat():
    alljobs = []
    templist = []
    process = subprocess.Popen("qstat -xml", shell=True, stdout=subprocess.PIPE)
    rawxml = process.communicate()[0]
    xmldict = xmltodict.parse(rawxml)
# qstat -xml puts pending and running in separate namespaces. Need to append. 
    for qkey in xmldict[u'job_info']:
        if qkey == u'@xmlns:xsd':
            continue
        else:
            if xmldict[u'job_info'][qkey]:
# necessary because xmltodict passes single entry lists as just the entry rather than a list
                if type(xmldict[u'job_info'][qkey][u'job_list']) is not list:
                    templist.append(xmldict[u'job_info'][qkey][u'job_list'])
                else:
                    templist = xmldict[u'job_info'][qkey][u'job_list']
                alljobs = alljobs + templist
    return alljobs


def launchall(runtime):
    sparktype = args.version
    #In LSF 1 slot = 1 core, + 16 for master
    slots = 16 + args.nnodes*16
    if sparktype == "stable": 
        sparktype = "current"
    if runtime == "None":
        options = "-n {}".format(slots)
    else:
        options = "-n {} -W {}".format(slots,runtime)
    #bsub requires argument for command, but the esub replaces it automatically
    output = subprocess.check_output(["bsub -a \"sparkbatch({})\" -J {} {} commandstring".format(sparktype,jobname,options)], shell=True)
    print 'Spark job submitted with {} workers ({} slots)'.format(args.nnodes, slots)
    jobID = output[1].lstrip("<").rstrip(">")
    return jobID

def launch(runtime):
    if not os.path.exists(os.path.expanduser('~/sparklogs')):
        os.mkdir(os.path.expanduser('~/sparklogs'))
    
    sparktype = args.version
    if sparktype == "stable":
        version = "current"
    else:
        version = str(sparktype)  
    masterjobID = startmaster(sparktype, runtime)
    time.sleep(10)
    try:
        for i in range(args.nnodes):
            startworker(sparktype, masterjobID, runtime)
    except:
        print "No workers specified or other error"
        traceback.print_exc()
        sys.exit()
    return masterjobID

def startmaster(sparktype, runtime):
    options = None
    if sparktype == "stable": 
        sparktype = "current"
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
    if masterURL is None:
        print "No master with the job id {} running or queued. Please submit master first.".format(masterjobID)
        sys.exit()
    if sparktype == "stable":
        sparktype = "current"
    if runtime is not None:
        command = "bsub -a \"spark(worker,{})\" -J W{} -W {} commandstring".format(sparktype,masterjobID,runtime)
    else:
        command = "bsub -a \"spark(worker,{})\" -J W{} commandstring".format(sparktype,masterjobID,runtime)
    os.system(command)

def login(nodeslots=16):
    if "MASTER" not in os.environ:
        masterlist = getallmasters()
        masterjobID = selectionlist(masterlist,'master')
        masterurl = "spark://{}:7077".format(getmasterbyjobID(masterjobID))
        print masterurl
        os.environ["MASTER"] = str(masterurl)
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = str(version) 
    if version not in os.environ['PATH']:
        os.environ["PATH"] = str("{}/bin:{}".format(version, os.environ['PATH']))
    print os.environ["MASTER"]
    print os.environ["SPARK_HOME"]
    print os.environ["PATH"]
    if nodeslots == 16:
        options = "16 -R \"sandy\""
    elif nodeslots == 32:
        options = "32"
    else: 
        print "You must request an entire node for a Driver job. Please request 16 or 32 slots."
        sys.exit()
    command = "bsub -Is -q interactive -n {} -env \"MASTER, SPARK_HOME, PATH\" /bin/bash".format(options)
    print command
    os.system(command)
    

def destroy(jobID):
    if jobID == None:
        print "Please specify a job ID for a master or cluster to tear it down."
        sys.exit()
    else:
        bkilljob(jobID)
        workers = []
        workers = getworkersbymasterID(jobID)
        if workers:
            for worker in workers:
                bkilljob(worker['jobid'])

def start():
    masters = findMasters()
    if len(masters) == 0:
        print >> sys.stderr, "No master found. If already reqest, please wait for master to be acquired. Otherwise, use spark-janelia launch."  
        sys.exit()
    else:
        master = 'spark://' + masters[0] + ':7077'

    if args.notebook is True or args.ipython is True:
       os.environ['PYSPARK_DRIVER_PYTHON'] = 'ipython'
    
    if args.notebook is True:
       os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
       os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook --ip "*" --port 9999 --no-browser'
       address = master[8:][:-5]
       print('\n')
       print('View your notebooks at http://' + os.environ['HOSTNAME'] + ':9999')
       print('View the status of your cluster at http://' + address + ':8080')
       print('\n')


    if os.getenv('PATH') is None:
        os.environ['PATH'] = ""

    os.system(version + '/bin/pyspark --master='+master)


def startScala():
    os.environ['SPARK_HOME'] = version

    if os.getenv('PATH') is None:
        os.environ['PATH'] = ""

    os.environ['PATH'] = os.environ['PATH'] + ":" + "/misc/local/python-2.7.11/bin"
    f = open(os.path.expanduser("~") + '/spark-master', 'r')
    os.environ['MASTER'] = f.readline().replace('\n','')
    os.system(version + '/bin/spark-shell')


def submit(master = ''):
    os.environ['SPARK_HOME'] = version

    if os.getenv('PATH') is None:
        os.environ['PATH'] = ""

    os.environ['PATH'] = os.environ['PATH'] + ":" + "/misc/local/python-2.7.11/bin"
    if master == '':
        with open(os.path.expanduser("~") + '/spark-master', 'r') as f:
            master = f.readline().replace('\n','')
    os.environ['MASTER'] = master

    # ssh into master and then run spark-submit
    currentPath = os.getcwd();
    command = 'ssh ' + master[8:14] +  ' "cd ' + currentPath + '; ' + version + '/bin/spark-submit --master ' + master + ' ' + args.submitargs + '"'
    os.system(command)


def launchAndWait():
        jobID  = launchall(str(args.sleep_time))
        master = ''     
        while( master == '' ):
            master = getmasterbyjobID(jobID)
            time.sleep(1) # wait 1 second to avoid spamming the cluster
            sys.stdout.write('.')
            sys.stdout.flush()
        return master, jobID

def update():
    currentdir = os.getcwd()
    scriptdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptdir)
    os.system('git pull origin master')
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
    print "Select {} to kill from list below:".format(jobtype)
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

def qdelmaster(masterjobID, skipcheckstop):
    jobtype = "master"
    workername = "W{}".format(masterjobID)
    if not skipcheckstop:
         skipcheckstop = checkstop(masterjobID,jobtype)
    if skipcheckstop:
        try:
            qdeljob(masterjobID)
        except:
            print "Master with job id {} not found".format(masterjobID)
            sys.exit(1)
        try:
            qdeljob(workername)
        except:
            print "Workers for master with job id {} failed to stop".format(masterjobID)
            sys.exit(1)
    else:
        print "As requested, master not stopped."
        sys.exit(0)


def submitAndDestroy( master, jobID ):
    submit(master)
    destroy(jobID)

if __name__ == "__main__":

    skipcheckstop = False

    parser = argparse.ArgumentParser(description="launch and manage spark cluster jobs")
                        
    choices = ('launch', 'launchall', 'login', 'destroy', 'start', 'start-scala', 'submit', 'lsd', 'launch-in', 'update', 'add-workers', 'remove-workers', 'stopcluster')
                        
    parser.add_argument("task", choices=choices)
    parser.add_argument("-n", "--nnodes", type=int, default=2, required=False)
    parser.add_argument("-i", "--ipython", action="store_true")
    parser.add_argument("-b", "--notebook", action="store_true")
    parser.add_argument("-v", "--version", choices=("stable", "rc", "test", "2"), default="stable", required=False)
    parser.add_argument("-j", "--jobID", type=int, default=None, required=False)
    parser.add_argument("-t", "--sleep_time", type=int, default=None, required=False)
    parser.add_argument("-s", "--submitargs", type=str, default='', required=False)
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("-d", "--driverslots", type=int, default=16, required=False)
                        
    args = parser.parse_args()
                        
    SPARKVERSIONS = {   
        'stable': '/misc/local/spark-current/',
        'rc': '/misc/local/spark-rc/',
        'test': '/misc/local/spark-test',
        '2': '/misc/local/spark-2'
    }                   
                        
    SPARKJOBS = {
        'stable': 'spark',   
        'rc': 'spark-rc',  
        'test': 'spark-test',   
        '2': 'spark-2'
    }    

    SPARKLAUNCHSCRIPTS = {
        'stable': 'start-cluster.sh',
        'rc': 'start-rc-cluster.sh',
        'test': 'start-test-cluster.sh',
        '2': 'start-2-cluster.sh'
    }

    version = SPARKVERSIONS[args.version]
    jobname = SPARKJOBS[args.version]
    launchscript = SPARKLAUNCHSCRIPTS[args.version]
                        
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
        startScala()   
                        
    elif args.task == 'submit':
        submit()        
                        
    elif args.task == 'lsd':
        master, jobID = launchAndWait()
        master = 'spark://%s:7077' % master
        print('\n')     
        print('%-20s%s\n%-20s%s' % ( 'job id:', jobID, 'spark master:', master ) )
        print('\n')     
        p = multiprocessing.Process(target=submitAndDestroy, args=(master, jobID))
        p.start()       

    elif args.task == 'launch-in':
        master, jobID = launchAndWait()
        print '\n\nspark master: {}\n'.format(master)
        login(int(args.driverslots))

    elif args.task == 'update':
        update()

    elif args.task == 'add-workers':
        for node in range(args.nnodes):
            startworker(args.version, str(args.jobID), args.sleep_time)

    elif args.task == 'remove-workers':
        terminatew = ""
        jobtype = "worker"
        for node in range(args.nnodes):
            workerlist = getworkersbymasterID(str(args.jobID))
            terminatew = stopworker(str(args.jobID), terminatew, workerlist, skipcheckstop)
   
    elif args.task == 'stopcluster':
        if args.jobID is not None:
            masterjobID = args.jobID
        else: 
            masterlist = getallmasters()
            masterjobID = selectionlist(masterlist,'master')
            skipcheckstop = True
        if not skipcheckstop:
            checkstop(masterjobjobID, 'master')
        destroy(masterjobID)

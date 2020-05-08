#!/bin/env python3

import threading
import os
import time
import sys
import datetime
import argparse
import sqlite3
import configparser
import subprocess
#from pprint import pprint

# so this app should first make the tensor of apps that need to run
# and create a todo list and then a todone list and then it should
# use the same techniques the launcher users to put apps on the right
# nodes and monitor their success or failure

# this should keep a database of the todo and todone on disk
# along with errored logging as checkpointing for restart

# it should also capture each ones stin stout and stderr
# and catalog those

# it could also do alert mailing for events (e.g. when it
# starts (even each time) and current progress towards completion
# send errors, send each completion, etc)

# Not sure if YAML is the right format for files...or just plain
# old X = CSVs

# stretch goal is setup app at start runs then
# even an array of commands and then final cleanup
# and aggrigation command (so not a full DAG but at least
# a simple start that most users can think about)

# super stretch goal...web interface or script construction wizzard

fatalError=False
dbFileName=None
taskLevel=0
maxTaskLevel=1

# this takes the list of tasks and, if no tasks are already in the 
# database, put stores them there

# need to add timestamping and such on transitions

# if the db file exists, it resets any RUNNING tasks to INIT
# state so they will run again.  

# at some point we might want to cleanup those directories of the 
# RUNNING tasks and check to see if there are new TASKS in the tasklist
# and append them but this is the first go

# tasks is a dictionary of each expanded command listed
# by their order in the original list e.g. command=x,y
# will be key 0 value [x] and key 1 value [y] where the 
# arrays are the full list of commands to be run with all
# parameters expanded
 
def createTaskList(tasks):
    global dbFileName
    db = sqlite3.connect(dbFileName)

    c = db.cursor()

    # if db file exists, reset the RUNNING to INIT

    if os.path.exists(dbFileName):
        sqlCmd = 'UPDATE tasks where status = "RUNNING" set status = "INIT"'


        c.execute(sqlCmd)
         
        return

    #otherwise start adding tasks to the table.      
    taskId=0
    c.execute("CREATE TABLE tasks (taskId integer, taskOrder integer, cmd text, status text, host text, startTime text, endTime text, lastUpdate text)")
    for key in tasks.keys():
        for cmd in tasks[key]:
            insertCmd="INSERT INTO tasks VALUES (%s,%s,'%s', 'INIT', '', '', '', '%s')"%(taskId,key,cmd,datetime.datetime.now())
            c.execute(insertCmd)
            taskId = taskId + 1
    db.commit()
    db.close()


# simply prints out the rows of all tasks including status

def getTaskList(dbFileName):
    db=sqlite3.connect(dbFileName)
    c=db.cursor()
    for row in c.execute("select * from tasks ORDER BY taskOrder"):
        print(row)


# this is the main thread that monitors task states
# and starts new ones.  It returns when there are no more 
# tasks in the INIT state
def runTasks():
    global dbFileName
    global taskLevel
    global maxTaskLevel
    try:
        db=sqlite3.connect(dbFileName)
        c=db.cursor()
        while not fatalError and taskLevel < maxTaskLevel:
            cmd = 'SELECT count(*) from tasks where status = "INIT"'
            c.execute(cmd)
            res = c.fetchone()
            print("Tasks remaining "+str(res[0]))
            if res[0] == 0:
                print("No more tasks....exiting startup thread")
                return(0)
            cmd = 'Select name from nodeThreads where running=0'
            c.execute(cmd)
            idleThreads=c.fetchone()
            if not idleThreads:
                time.sleep(1)
            else:
                for hostThread in idleThreads:
                    sqlCmd = 'SELECT cmd from tasks where status = "INIT" AND taskOrder = ' + str(taskLevel)
                    c.execute(sqlCmd)
                    cmd = c.fetchone()
                    if not cmd:
                        sqlCmd = 'Select count(name) from nodeThreads where running=0'                
                        c.execute(sqlCmd)
                        numRunning = c.fetchone()[0]
                        if numRunning == 0:
                           taskLevel = taskLevel + 1
                    else:
                        taskThread = threading.Thread(target=startTask,args=(cmd[0],hostThread))
                        taskThread.start()
                        running = 0
                        while not running:
                            sqlCmd= 'SELECT running from nodeThreads where name = "' + hostThread +'"'
                            c.execute(sqlCmd)
                            running = c.fetchone()[0] 
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


# this is the function that runs a task.  It does a bruit force
# ssh to the node given in threadname (where threadname is the
# merger of the nth task on a node and the node name colon separated
# but in this backwards order so we distribute evenly across nodes
# rather than fill up one node and then the next)

def startTask(cmd, threadname):
    global fatalError
    global dbFileName
    logRunning(cmd,threadname)
    try:
        # get rid of characters that will make our lives painful in driectory names
        # and use that name as the directory in which that specific command will run
        fname=cmd.replace(' ','_')
        fname=fname.replace('"','')
        fname=fname.replace('\'','')
        fname=fname.replace('\\','')
        fname=fname.replace('/','')
        fname=fname.replace('!','')
        fname=fname.replace(';','')
        fname=fname.replace('>','')
        fname=fname.replace('<','')
        if not os.path.isdir(fname):
            os.mkdir(fname)
        cwd=os.getcwd() + '/' + fname
        # so send stdout and stderr to files in the tasks directory and run the 
        # task in that directory by brute force 
        hostname=threadname.split(':')[1]
        stdoutFile =  cwd+'/output'
        stderrFile= cwd+'/errors'
        sshcmd='ssh ' + hostname + ' cd ' +cwd+' && '+ cmd +" > " + stdoutFile + ' 2> '+stderrFile
  
         
        status=subprocess.run(sshcmd.split(' '),capture_output=False)

        # log exit
        exitCode=status.returncode 
        logTaskCompletion(exitCode, cmd, threadname)
    except:
        # if something goes wrong with this...something bad happened so throw on the breaks
        # with the fatalError flag 
        # so we dont start any new tasks

        print("Unexpected error:", sys.exc_info()[0])
        logTaskCompletion(1,cmd,threadname)
        fatalError=True
        raise


# logs a task as running on a specific threadname location (thnead number : hostname)
# and pull the thread out of the available pool
def logRunning(cmd,threadname):
    global dbFileName
    print('Starting task ' + cmd)
    db = sqlite3.connect(dbFileName)
    c = db.cursor()
    cmd='UPDATE tasks SET status = "RUNNING", host = "'+threadname+'" where cmd = \''+cmd+'\''
    c.execute(cmd)
    cmd='UPDATE nodeThreads SET running=1 where name = "'+threadname+'"'
    c.execute(cmd)
    db.commit()
    db.close()


# log a completed task and put that thread back into the available pool
def logTaskCompletion(exitCode, command, threadname):
    global dbFileName
    db = sqlite3.connect(dbFileName)
    c = db.cursor()
    # do the DB update
    if exitCode == 0:
        # log completion
        print("Task completed successfully " + command)
        cmd='UPDATE tasks SET status = "Complete" where cmd = \''+command+'\''
        c.execute(cmd)
    else:
        # log errored
        print("Task errored " + command)
        cmd='UPDATE tasks SET status  = "Errored" where cmd = \''+command+'\''
        c.execute(cmd)
    cmd='UPDATE nodeThreads SET running = 0 where name = "'+threadname+'"'
    c.execute(cmd)
    
    db.commit()
    db.close()


# this is the parameter expander.  for each parameter and command it expands 
# the list of tasks to run into a larger list

def expandParam(baseCommands,tag,values):
    list = {}
    # need to expand file and range parameters here
    newvals=[]
    for value in values:
        # see if this is values in a file
        if value.startswith('<') and value.endswith('>'):
           newvals.remove(value)
           filename = value[1:-1]
           file=open(filename,'r')
           fileValues = file.readlines()
           if len(fileValues) == 0:
               print("no values found in file " + filename)
               sys.exit(1)
           for val in fileValues:
               newvals.append(val)
        # or values in a range
        elif value.startswith('[') and value.endswith(']'):
           newvals.remove(value)
           rangeString=value[1:-1]
           if rangeString.count(':') != 2:
              print("Range misformated.  Must be 3 : separated numbers " + rangeString)
              sys.exit(1)
           (start,end,step)=value.split(':')
           fstart=None
           fend=None
           fstep=None
           # basic error checking
           try:
               fstart=float(start)
           except:
               print("Range start of " + start + " is not numeric")
               sys.exit(1) 
           try:
               fend=float(end)
           except:
               print("Range start of " + end + " is not numeric")
               sys.exit(1) 
           try:
               fstep=float(step)
           except:
               print("Range start of " + step + " is not numeric")
               sys.exit(1) 
           if fstep == 0:
               print("Error...step size cannot be 0 for a range of values")
               sys.exit(1)
           fv = fstart
           if fstep < 0:
               while fv >= fend:
                   newvals.append(fv)
                   fv=fv+fstep
           else:
               while fv <= fend:
                   newvals.append(fv)
                   fv=fv+fstep
        # or just single values.  Note you can mix and match file, ranges, and single values
        else:
           newvals.append(value) 
   
            
    for key in baseCommands.keys():
        list[key]=[]
        for value in newvals:
            for s in baseCommands[key]:
                expandedString = s.replace(tag,value)
                if expandedString not in list[key]:
                    list[key].append(expandedString)
    return list


# loads and checks the validity of the ini file
def loadIni(configFile):
    global maxTaskLevel
    cmd={}
    parameters=[]
 
    config = configparser.ConfigParser()
    config._interpolation = configparser.ExtendedInterpolation()
    iniFile=config.read(configFile)
    if len(iniFile) == 0:
        print("No configuration file found.  Please run with the --writeConfig flag")
        print("   to create a new config file.")
        sys.exit(1)
    sweep=config.items("sweep info")
    sysConf=config.items("system config") 
    for param in sweep:
        key=param[0]
        if key == 'command':
            cmds=param[1].split(',')
            for c in cmds:  
                cmd[cmds.index(c)]=[c]
                maxTaskLevel = maxTaskLevel + 1
            continue
        parameters=sweep 
    # now check that cmd has all paarameters and visa versa
    nParam=len(parameters)
    for i in range(0,nParam):
        if parameters[i]==None or parameters[i][0]=='name' or parameters[i][0]=='command':
            continue
        cmd=expandParam(cmd,parameters[i][0],parameters[i][1].split(','))
    cfg={}
    for p in sysConf:
        if len(p) < 1:
           print("Bad config line " + p)
        else:
           cfg[p[0]]=p[1] 
    
    return cmd,cfg



# create and submits the simple slurm script that loads the modules
# and runs this script in --server mode

def submitSlurmJob(jobname,account, queueName, ntasks, nnodes, jobTime, modules):
    slurmScript="""#!/bin/bash
#
# Simple SLURM script for submitting multiple serial
# jobs (e.g. parametric studies) using a script wrapper
# to launch the jobs.
#
#------------------Scheduler Options--------------------
"""
    try:
        test=int(nnodes)
    except:
        print("Number of nodes must be an integer not " + nnodes)
        sys.exit(1)
    try:
        test=int(ntasks)
    except:
        print("Number of tasks on a node must be an integer not " + ntasks)
        sys.exit(1) 
    try:
        if jobTime.count(':') != 2:
            print("Job time must be formatted in hh:mm:ss format not " + jobTime)
            sys.exit(1)
        x=jobTime.split(':')
        for y in x:
            test=int(y)
    except:
        print("Job time must be formatted in hh:mm:ss format not " + jobTime)
        sys.exit(1)
# should probably look at jobname and scrub it for bad characters
# and call sinfo to get the list of slurm queues and make sure this is valid
# but Im bored now
    slurmScript = slurmScript + "#SBATCH -J " + jobname +"\n"
    slurmScript = slurmScript + "#SBATCH -N " + nnodes +"\n"
    slurmScript = slurmScript + "#SBATCH -n " + ntasks +"\n"
    slurmScript = slurmScript + "#SBATCH -p " + queueName +"\n"
    slurmScript = slurmScript + "#SBATCH -o " + jobname +".o%j\n"
    slurmScript = slurmScript + "#SBATCH -t " + jobTime +"\n"
    slurmScript = slurmScript + "#SBATCH -A " + account +"\n"
    for module in modules.split(','):
       slurmScript=slurmScript+"module load " + module +"\n"
    slurmScript = slurmScript+"runner.py --server "+jobname+"\n"
    sfile=open(jobname+".slurm",'w')
    sfile.write(slurmScript)
    sfile.close()
    os.exec("sbatch " + slurmScript)
    return


# this writes the exemplar ini file
def writeConfig(fname):

    iniString="""[sweep info]
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
"""
    f=open(fname,'w')
    f.write(iniString)

# parse the command line args
def parseArgs():
    parser = argparse.ArgumentParser(description='Parametric Sweeper')
    parser.add_argument("configFile", help="run a slurm parameter sweep from this configuraion file")
    parser.add_argument('--server',  action="store_true",
                   help='run the server (not to be run by users but by SLURM')
    parser.add_argument('--status',  action="store_true",
                   help='get the status of the current sweep')
    parser.add_argument('--reset',  action="store_true",
                   help='reset parameter sweep')
    parser.add_argument('--writeConfig', action="store_true",
                   help='write a new config file')
    return parser.parse_args()

# calls to scontrol to get the list of nodes 
# and then creates tasks based on the tasks per node
# in the ini file.  This creates the task db
# used to start and monitor state of threads
def getNodeList(tasksPerNode):
    global dbFileName
    snl=os.getenv('SLURM_NODELIST')
    query='scontrol show hostname '+snl
    slurmquery=subprocess.run(query.split(' '),encoding='utf-8',capture_output=True)
    nlist = slurmquery.stdout.split('\n');
    db = sqlite3.connect(dbFileName)
    c = db.cursor()
    try:
        c.execute('DROP TABLE nodeThreads')
    except:
        pass
    c.execute("CREATE TABLE nodeThreads (name text, running int)")
    for key in nlist:
        if not key:
            continue
        for tasknum in range(0,int(tasksPerNode)):
            threadname=str(tasknum)+':'+key
            insertCmd='INSERT INTO nodeThreads VALUES ("%s",%i)'%(threadname,0)
            c.execute(insertCmd)
    db.commit()
    db.close()


# the main...
def main():
    global dbFileName
    args=parseArgs()
    configFile = args.configFile
    cfgFile=configFile
    jobName=configFile
    # add .ini to the config file name if its not there
    # and remove it from the name of this job if it is
    if not configFile.endswith('.ini'):
        cfgFile=configFile+'.ini'
    else
        jobName=jobName[:-4]
    # ok have a switch here so if run by default it loads
    # the ini file and creates the slurm job to submit
    # to the cluster

    # have a --server flag so it then runs on the master node
    # from the slurm job which then checks the DB...loads or
    # initializes state and then starts running jobs
    if args.writeConfig:
        writeConfig(cfgFile)
        sys.exit(0)
    dbFileName = os.getcwd()+'/'+jobName+'.sqllite'
    if args.reset:
        print("Resetting everything...")
        os.remove(dbFileName)
    tasks,cfg=loadIni(cfgFile)
    createTaskList(tasks)
    if args.server:
        getNodeList(cfg["num_tasks_per_node"])
        runTasks()
    elif args.status:
        getTaskList()
    else:
        # need to check validity of values here....all in good time
        submitSlurmJob(jobName, cfg['allocation'], cfg['queue_name'], 
		cfg['num_tasks_per_node'], cfg['num_nodes'], cfg['job_time'], cfg['modules'])


if __name__ == "__main__":
    main()

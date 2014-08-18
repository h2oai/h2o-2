#!/bin/bash


# force an interactive shell to get JAVA_HOME set?

remote_setup() {
    REMOTE_IP=$1
    REMOTE_USER=0xcustomer@$REMOTE_IP
    REMOTE_SCP="scp -i $HOME/.0xcustomer/0xcustomer_id_rsa"
    # FIX! I shouldn't have to specify JAVA_HOME for a non-iteractive shell running the hadoop command?
    # but not getting it otherwise on these machines
    REMOTE_SSH_USER="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER export JAVA_HOME=/usr/lib/jvm/java-7-oracle;"
}

remote_list() {
    $REMOTE_SSH_USER "hadoop job -list"
}

remote_kill() {
    #***********************************************************************************
    echo "Does 0xcustomer have any hadoop jobs left running from something? (manual/jenkins/whatever)"
    rm -f /tmp/my_jobs_on_hadoop_$REMOTE_IP

    echo "Checking hadoop jobs"
    $REMOTE_SSH_USER 'hadoop job -list' > /tmp/my_jobs_on_hadoop_$REMOTE_IP
    cat /tmp/my_jobs_on_hadoop_$REMOTE_IP

    echo "kill any running hadoop jobs by me"
    while read jobid state rest
    do
        echo $jobid $state
        # ignore these kind of lines
        # 0 jobs currently running
        # JobId   State   StartTime   UserName    Priority    SchedulingInfo
        if [[ ("$jobid" != "JobId") && ("$state" != "jobs")  && ("$jobid" != "Total") ]]
        then
            echo "hadoop job -kill $jobid"
            $REMOTE_SSH_USER "hadoop job -kill $jobid"
        fi
    done < /tmp/my_jobs_on_hadoop_$REMOTE_IP

    #*********************************************************************************
}

# hdp2.1
remote_setup 192.168.1.187
remote_kill
remote_list


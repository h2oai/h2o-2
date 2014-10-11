#!/bin/bash


# force an interactive shell to get JAVA_HOME set?

remote_setup() {
    REMOTE_IP=$1
    REMOTE_USER=0xcustomer@$REMOTE_IP
    REMOTE_SCP="scp -i $HOME/.0xcustomer/0xcustomer_id_rsa"
    # FIX! I shouldn't have to specify JAVA_HOME for a non-iteractive shell running the hadoop command?
    # but not getting it otherwise on these machines
    SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-7-oracle; "
    REMOTE_SSH_USER_WITH_JAVA="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER $SET_JAVA_HOME"
}

remote_list() {
    $REMOTE_SSH_USER_WITH_JAVA "mapred job -list"
}

remote_kill() {
    #***********************************************************************************
    echo "Does 0xcustomer have any hadoop jobs left running from something? (manual/jenkins/whatever)"
    rm -f /tmp/my_jobs_on_hadoop_$REMOTE_IP

    echo "Checking hadoop jobs"
    $REMOTE_SSH_USER_WITH_JAVA 'mapred job -list' > /tmp/my_jobs_on_hadoop_$REMOTE_IP; chmod 777 /tmp/my_jobs_on_hadoop_$REMOTE_IP
    cat /tmp/my_jobs_on_hadoop_$REMOTE_IP

    echo "kill any running hadoop jobs by me"
    while read jobid state starttime username rest
    do
        echo $jobid $state
        # ignore these kind of lines
        # 0 jobs currently running
        # JobId   State   StartTime   UserName    Priority    SchedulingInfo
        # if [[ ("$jobid" != "JobId") && ("$state" != "jobs")  && ("$jobid" != "Total")  && ("$username" == "0xcustomer") ]]
        if [[ ("$username" == "0xcustomer") ]]
        then
            echo "mapred job -kill $jobid"
            $REMOTE_SSH_USER_WITH_JAVA "mapred job -kill $jobid"
        fi
    done < /tmp/my_jobs_on_hadoop_$REMOTE_IP

    #*********************************************************************************
}

# hdp2.1
remote_setup mr-0xd8-precise1
# shouldn't it kill more than one at a time?
remote_kill


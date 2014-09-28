
#***********************************************************************************
echo "Does 0xcustomer have any hadoop jobs left running from something? (manual/jenkins/whatever)"
rm -f /tmp/my_jobs_on_hadoop_$REMOTE_IP

echo "Checking hadoop jobs"
$REMOTE_SSH_USER 'hadoop job -list' > /tmp/my_jobs_on_hadoop_$REMOTE_IP; chmod 777 /tmp/my_jobs_on_hadoop_$REMOTE_IP
cat /tmp/my_jobs_on_hadoop_$REMOTE_IP

echo "kill any running hadoop jobs by me"
while read jobid state rest
do
    echo $jobid $state
    # ignore these kind of lines
    # 0 jobs currently running
    # JobId   State   StartTime   UserName    Priority    SchedulingInfo
    if [[ ("$jobid" != "JobId") && ("$state" != "jobs") && ("$jobid" != "Total") ]]
    then
        echo "hadoop job -kill $jobid"
        $REMOTE_SSH_USER "hadoop job -kill $jobid"
    fi
done < /tmp/my_jobs_on_hadoop_$REMOTE_IP


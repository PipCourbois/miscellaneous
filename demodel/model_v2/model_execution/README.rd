----
**DEACT MODEL**
----

Here you can find the code used to execute the churn model. It runs every day at this directory: 
ebates@cerebro-clv-job-submitter1.snc1:/home/ebates/crontabJobs/DeactModel

Crontab commmand is as follows:

12 01 * * * nohup sh -x /home/ebates/crontabJobs/DeactModel/DeactRun.sh > /home/ebates/crontabJobs/DeactModel/log.out 2>&1

If you have any questions, feel free to reach out at ebates@groupon.com

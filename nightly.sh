#!/bin/bash
source /home/ruser/.bashrc
cd /data/sdb

bucket=`cat Monthly_Report.yaml | yq -r .bucket`

key='SamLastRun'
WaitUntilHour=12

SamLastRunDate=`aws s3 ls s3://$bucket/$key | head -n 1 | cut -d " " -f1`
SamLastRunTime=`aws s3 ls s3://$bucket/$key | head -n 1 | cut -d " " -f2`
TodaysDate=$(TZ=America/New_York date +%Y-%m-%d)
CurrentHour=$(TZ=America/New_York date +%H)

# Wait until SamLastRun file timestamp has today's date
# Or until the hour reaches `WaitUntilHour`, whichever comes first.
# i.e., don't keep waiting after `WaitUntilHour` is reached. Give up at that point.

while [ ${TodaysDate} != ${SamLastRunDate} ] && [ ${CurrentHour} -lt ${WaitUntilHour} ]; do
    echo "${TodaysDate} (today) <> ${SamLastRunDate} (sam last run) and ${CurrentHour} (this hour) < ${WaitUntilHour} (wait until hour)  | waiting 10 minutes"
    sleep 600
    SamLastRunDate=`aws s3 ls s3://$bucket/$key | head -n 1 | cut -d " " -f1`
    SamLastRunTime=`aws s3 ls s3://$bucket/$key | head -n 1 | cut -d " " -f2`
    TodaysDate=$(TZ=America/New_York date +%Y-%m-%d)
    CurrentHour=$(TZ=America/New_York date +%H)
done
echo "Today's date is ${TodaysDate}. Sam Last Run date is ${SamLastRunDate}. Current time is $(TZ=America/New_York date +%H:%M:%S). Proceed."
# Once the condition is met to exit the while loop,
# check to make sure SamLastRunDate is today's date and then go.
if [ "${TodaysDate}" == "${SamLastRunDate}" ]; then
    # Run script
    echo $SamLastRunTime -  Run Scripts
    Rscript Monthly_Report_Calcs_1.R
    Rscript Monthly_Report_Calcs_2.R
    Rscript Monthly_Report_Package_1.R
    Rscript Monthly_Report_Package_2.R
    Rscript get_alerts.R
    echo "------------------------"

    cd SigOps
    Rscript Monthly_Report_Package.R
    Rscript get_alerts_sigops.R
    echo "------------------------"
    Rscript Monthly_Report_Package_1hr.R
    echo "------------------------"
    Rscript Monthly_Report_Package_15min.R
    echo "------------------------"
    Rscript checkdb.R  # Query the database for records by date for monitoring.
    cd ..
fi




# Run User Delay Cost on the SAM on the 1st, 11th and 21st of the month
if [[ $(date +%d) =~ 01|11|21 ]]; then
    ~/miniconda3/bin/conda run -n sigops python user_delay_costs.py
fi

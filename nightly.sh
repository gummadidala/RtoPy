#!/bin/bash
source /home/ruser/.bashrc
cd /data/sdb

# Python executable path (direct path to Python in sigops environment)
# Option 1: Direct path (recommended - faster, no conda overhead)
PYTHON_CMD="~/miniconda3/envs/sigops/bin/python"

# Option 2: If using conda run (slower but safer)
# PYTHON_CMD="~/miniconda3/bin/conda run -n sigops python"

# Option 3: If using system Python (must have all packages installed)
# PYTHON_CMD="python"

# Create timestamped log file
TIMESTAMP=$(TZ=America/New_York date +%Y%m%d_%H%M%S)
LOGFILE="nightly_${TIMESTAMP}.log"

# Redirect all output to log file (both stdout and stderr)
exec > >(tee -a "$LOGFILE") 2>&1

echo "=========================================="
echo "NIGHTLY PIPELINE STARTED"
echo "Timestamp: $(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S')"
echo "Log file: $LOGFILE"
echo "Python: $PYTHON_CMD"
echo "=========================================="
echo ""

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
    # Run Python scripts (converted from R)
    echo $SamLastRunTime - Run Monthly Report Python Pipeline
    echo "========================================"
    echo "STEP 1/5: Monthly Report Calcs 1"
    echo "========================================"
    $PYTHON_CMD monthly_report_calcs_1.py
    if [ $? -eq 0 ]; then
        echo "✅ Calcs 1 Complete"
        echo ""
        echo "========================================"
        echo "STEP 2/5: Monthly Report Calcs 2"
        echo "========================================"
        $PYTHON_CMD monthly_report_calcs_2.py
        if [ $? -eq 0 ]; then
            echo "✅ Calcs 2 Complete"
            echo ""
            echo "========================================"
            echo "STEP 3/5: Monthly Report Package 1"
            echo "========================================"
            $PYTHON_CMD monthly_report_package_1.py
            if [ $? -eq 0 ]; then
                echo "✅ Package 1 Complete"
                echo ""
                echo "========================================"
                echo "STEP 4/5: Monthly Report Package 2"
                echo "========================================"
                $PYTHON_CMD monthly_report_package_2.py
                if [ $? -eq 0 ]; then
                    echo "✅ Package 2 Complete"
                    echo ""
                    echo "========================================"
                    echo "STEP 5/5: Get Alerts"
                    echo "========================================"
                    $PYTHON_CMD get_alerts.py
                    if [ $? -eq 0 ]; then
                        echo ""
                        echo "=========================================="
                        echo "🎉 COMPLETE PIPELINE FINISHED SUCCESSFULLY! 🎉"
                        echo "=========================================="
                        echo "End time: $(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S')"
                        echo "All logs saved to: $LOGFILE"
                        echo ""
                    else
                        echo "❌ Get Alerts failed - check get_alerts.log"
                        exit 1
                    fi
                else
                    echo "❌ Package 2 failed - check monthly_report_package_2.log"
                    exit 1
                fi
            else
                echo "❌ Package 1 failed - check monthly_report_package_1.log"
                exit 1
            fi
        else
            echo "❌ Calcs 2 failed - check monthly_report_calcs_2.log"
            exit 1
        fi
    else
        echo "❌ Calcs 1 failed - check monthly_report_calcs_1.log"
        exit 1
    fi
    echo "------------------------"

    # TODO: SigOps Python scripts (when converted, uncomment below)
    # cd SigOps
    # $PYTHON_CMD monthly_report_package.py
    # $PYTHON_CMD get_alerts_sigops.py
    # $PYTHON_CMD monthly_report_package_1hr.py
    # $PYTHON_CMD monthly_report_package_15min.py
    # $PYTHON_CMD checkdb.py
    # cd ..
else
    echo "=========================================="
    echo "SKIPPING: SamLastRun date (${SamLastRunDate}) does not match today (${TodaysDate})"
    echo "or wait time exceeded (current hour: ${CurrentHour}, wait until: ${WaitUntilHour})"
    echo "=========================================="
fi




# Run User Delay Cost on the SAM on the 1st, 11th and 21st of the month
if [[ $(date +%d) =~ 01|11|21 ]]; then
    echo ""
    echo "========================================"
    echo "Running User Delay Costs (scheduled for 1st, 11th, 21st)"
    echo "========================================"
    $PYTHON_CMD user_delay_costs.py
    if [ $? -eq 0 ]; then
        echo "✅ User Delay Costs Complete"
    else
        echo "❌ User Delay Costs failed"
    fi
fi

# Final completion message
echo ""
echo "=========================================="
echo "NIGHTLY SCRIPT COMPLETED"
echo "End time: $(TZ=America/New_York date '+%Y-%m-%d %H:%M:%S')"
echo "Log file: $LOGFILE"
echo "=========================================="

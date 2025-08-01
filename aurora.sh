
export LC_ALL="en_US.UTF-8"

host=`cat Monthly_Report_AWS.yaml | yq -r .RDS_HOST`
dbname=`cat Monthly_Report_AWS.yaml | yq -r .RDS_DATABASE`
user=`cat Monthly_Report_AWS.yaml | yq -r .RDS_USERNAME`
password=`cat Monthly_Report_AWS.yaml | yq -r .RDS_PASSWORD`

echo $host
echo $dbname
echo $user

mysql -h $host -u $user -D $dbname -P 3306 -p$password

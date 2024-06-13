#!/bin/bash

config_file="./src/assets/config.json"
backup_config_file="../config_backup/config.json"

token=$(hexdump -n16 -e'4/4 "%08x" 1 "\n"' /dev/urandom)
read -p "Please enter a username for this user: " user

cat $config_file | jq '.whitelist += {"'"$token"'":{"permissions":["basic"],"user":"'"$user"'"}}' > $backup_config_file
cat $backup_config_file > $config_file

echo "
Token: $token
User: $user

The user has been added to the whitelist with basic permissions. \
Advanced permissions must be added manually. Please provide this token to the user."

read -p "restart the API? [Y/n]: " restart
restart=${restart:-y}
restart=$(echo $restart | tr '[:upper:]' '[:lower:]')
if [ "$restart" == "y" ]; then
    echo "The API will now restart..."
    /bin/bash docker_redeploy.sh
fi
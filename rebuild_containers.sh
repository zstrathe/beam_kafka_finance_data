#echo "Starting data pipeline services..."

docker compose build --no-cache # rebuild the container images to ensure any changes are pushed
echo "Finished rebuilding container images!"

# force recreate the container images (in case any are currently running)
# and launch compose watch in a new terminal window assumes that running gnome-terminal (i.e., Ubuntu)
#docker compose up --force-recreate --remove-orphans && gnome-terminal -x sh -c "docker compose watch"

#docker compose up --force-recreate --remove-orphans &
#gnome-terminal -x sh -c "echo Waiting 5 seconds to start compose watch; sleep 5; docker compose watch" &


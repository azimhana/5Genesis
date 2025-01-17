
1. Install docker, the version does not matter as long as it is at least docker 18.06.0

2. Activate docker swarm. If you are unsure on how to do it then you can run the command "docker system info" and then look for "Swarm: xxx". To active docker swarm, run the command "docker swarm inti". 

4. Clone this repository.

5. Create the “analytics_connections” file with this format. 

docker secret create analytics_connections - << END
platform_name:      # replace platform_name, e.g. uma
  host: ip        # replace ip, e.g. 192.168.0.1
  port: p         # replace p with the port, e.g. 8080
  user: u         # replace u with the user name, e.g. user1
  password: pw    # replace pw with the password
  databases:
    - db_name1      # replace db_name1 with your database name(s)
    - db_name2      # replace or delete if there is only one database
END
Note that the choice of values must the same for your ".yaml" -and the "analytics_connections” -file. 

6. Create a docker swarm secret by running this: 
“echo secret123 | docker secret create analytics_secret -”. Replace "secret123" with whatever you want.  


7. Install influxdb with docker. It is recomeded to do the configuration withing the ".yaml" file. 
services:
  influxdb:
    image: influxdb:1.6
    ports:
      - "8086:8086"
    environment: 
      INFLUXDB_HTTP_AUTH_ENABLED: "true"   
      INFLUXDB_ADMIN_USER: "isakl"   
      INFLUXDB_ADMIN_PASSWORD: "qwertyui"
    volumes:
      - /home/isak/Analytics/influxdb.conf:/etc/influxdb/influxdb.conf
      - /home/isak/influxdb_data:/var/lib/influxdb

8. Run “./Analytics/install.sh”. This will create and start all containers.

9. Run "docker service ls" to see if all the containers are up and running



## Environment setting up: 

    PS1="> " # Adding this variable to .bashrc to create a new line when using the terminal.

Setting up: 

    pip install uv # Install uv

    uv init -p 3.12 # Initialize a virtual environment with python

    uv add kafka-python pandas pyarrow # Installing required dependencies

    uv add --dev jupyter # Add jupyter as dev depency

Specifing the python environment to use: 

    .venv/bin/python # When creating a uv, the path where the specific python version is stored. 

IMPORTANT: To avoid problems when using virtual environments, its recommended just work within the dir project when we created the uv, if not it probably will not recognize our uv and can use any other within our system files

See all the logs when initializing an image: 

    sudo docker compose logs <image> -f

Create, delete or describe topics in Redpanda: 

    sudo docker exec -it <redpanda-container-name> rpk topic create <topic-name>

    sudo docker exec -it <redpanda-container-name> rpk topic delete <topic-name>

    sudo docker exec -it <redpanda-container-name> rpk topic describe -a <topic-name>

To use a dependency/packages without installing it we can use uvx: 

    uvx pgcli -h localhost -p 5432 -U postgres -d postgres
    
drivers to connect to the postgreSQL database: 

    uv add psycopg2-binary


### PyFlink: 

Flink is a streaming processing framework/platform that take care retrying events when there is an error or when events arrived late. 

When using Flink our consumers will not be running locally, instead, will be running in the Flink's cluster. 

Setting up PyFlink: 

    # Downloading required files:

    PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"

    wget ${PREFIX}/Dockerfile.flink
    wget ${PREFIX}/pyproject.flink.toml
    wget ${PREFIX}/flink-config.yaml

    
    # chon (daniel) src

IMPORTANT: When we are creating the flink image, we need to set up all drivers for the different connections we will be using (eg. PostgreSQL db, s3, gcs, redshift, bigquery, etc.) 

Executing a Flink job: 

    # example: 
    sudo docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/pass_through_job.py \
    --pyFiles /opt/src -d

    # example 2: 
    docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/aggregation_job.py \
    --pyFiles /opt/src -d
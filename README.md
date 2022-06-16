# Sage Prefect Tasks

Python package of useful Prefect tasks for common use cases at Sage Bionetworks.

Inspired by [Pocket/data-flows](https://github.com/Pocket/data-flows).

## Demo Flow

![Demo Flow](flow.png)

## Demo Usage

### Getting set up

```sh
# Create a virtual environment with the Python dependencies
pipenv install

# Copy the example `.env` file and update the auth tokens
cp .env.example .env
```

### Run the flow at the command line

You'll need to [get set up](#getting-set-up) first.

```sh
# Run the demo (pipenv will automatically load the `.env` file)
pipenv run python demo.py
```

### Inspect the flow using the Prefect Server UI

You'll need to [get set up](#getting-set-up) first.

```sh
# Deploy Prefect Server in the background
prefect server start --detach

# Create a project in Prefect Server
prefect create project "demo"

# Run the demo in "register" mode
pipenv run python demo.py register

# Explore the flow under the demo project in Prefect Server
# Usually hosted at http://127.0.0.1:8080/

# Stop the running containers
prefect server stop
```

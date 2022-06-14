# Sage Prefect Tasks

Python package of useful Prefect tasks for common use cases at Sage Bionetworks.

Inspired by [Pocket/data-flows](https://github.com/Pocket/data-flows).


## Demo guide

1. Install and load dependencies

    ```
    pipenv install
    pipenv shell
    ```

1. (Optional) To use the prefect server. Instructions [here](https://docs.prefect.io/orchestration/) and access http://localhost:8080/

    ```
    prefect backend server
    prefect server start
    prefect create project 'demo'
    ```

1. Make edits to the `demo.py` if you are using the prefect server, then execute.

    ```
    python demo.py
    ```

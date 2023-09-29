The content demonstrates how to perform unit testing in Apache Airflow.

We expect you to have a moderate level of proficiency in:

- Apache Airflow
- Python
- testing in Python and pytest

To run the examples from this repository, you need:

1. Make sure you have Python 3.10+ and Docker desktop on your machine
2. Clone the repository
3. Create a python virtual environment with Python 3.10+
4. Install requirement.txt (for example with the command `pip install -r requirements.txt`)
5. Start docker compose with postgres and minio (for example with the command `docker compose up`)
6. Run tests in test folder with pytest

To clean up:

1. Stop and delete containers (for example with the command `docker compose down`)

To better understand the subject please refer to these articles:

- [The Testing Pyramid](https://medium.com/contino-engineering/knowthe-testing-pyramid-42a4b3573988)
- [Mastering Unit Tests in Python with pytest: A Comprehensive Guide](https://medium.com/@adocquin/mastering-unit-tests-in-python-with-pytest-a-comprehensive-guide-896c8c894304)
- [Unit Testing in Data Engineering: A Practical Guide](https://medium.com/@samzamany/unit-testing-in-data-engineering-a-practical-guide-91196afdf32a)
- [Airflow hooks](https://docs.astronomer.io/learn/what-is-a-hook)
- [Datasets and data-aware scheduling in Airflow](https://docs.astronomer.io/learn/airflow-datasets)
- [DAG scheduling and timetables in Airflow](https://docs.astronomer.io/learn/scheduling-in-airflow)


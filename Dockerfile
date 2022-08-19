FROM python:3.10.6

WORKDIR /usr/src/app

ARG PSEUDO_VERSION=1

COPY . .

RUN pip install --no-cache-dir pipenv

RUN SETUPTOOLS_SCM_PRETEND_VERSION=${PSEUDO_VERSION} \
	pipenv install --system

CMD [ "python", "-c", "import sagetasks" ]

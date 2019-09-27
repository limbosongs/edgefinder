FROM continuumio/anaconda

RUN mkdir /code

WORKDIR /code

# --------------------- Python libraries -------------------------------------
ADD ./requirements /code/requirements

RUN pip install -r requirements/base.txt

ADD ./app /code/app


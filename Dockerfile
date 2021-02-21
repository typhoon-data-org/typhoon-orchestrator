FROM python:3.6

ENV PROJECT_NAME="typhoon_project"
ENV TYPHOON_HOME="/opt/$PROJECT_NAME/"

COPY . /
RUN pip install -e ./[dev]

RUN cd /opt/ && typhoon init $PROJECT_NAME
WORKDIR $TYPHOON_HOME

# command to run on container start
CMD ["bash"]

FROM python:3.9.12
RUN apt-get update && apt-get -y install cron vim
ADD . /bigdata
WORKDIR /bigdata
#to COPY the remote file at working directory in container
COPY crontab /etc/cron.d/crontab
RUN pip install -r requirements.txt
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
#CMD instruction should be used to run the softwarecontained by your image, along with any arguments.
#CMD [ "python", "./etl.py"]

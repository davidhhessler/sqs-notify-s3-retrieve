FROM python:2-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app/

# Volume where S3 data will arrive by default
VOLUME /usr/src/app/incoming

ENTRYPOINT ["python", "s3_retrieve.py"]
CMD ["-h"]
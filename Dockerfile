FROM python
RUN pip3 install zeebe-grpc SPARQLWrapper PyMysql
ENV ZEEBE_GATEWAY zeebe-zeebe-gateway:26500
ENV JENA_SERVER jena_jena:3030
ENV MINIO_SERVER minio:30002
ENV MYSQL_SERVER mysql:30390
ENV MYSQL_USER   VV0JquD7JL
ENV MYSQL_PASSWD 2ZEzvefSKm
ENV MYSQL_DB     cpps_db
COPY main.py .
ENTRYPOINT ["python3", "-u", "main.py"]
FROM python
RUN pip3 install zeebe-grpc SPARQLWrapper PyMysql
ENV ZEEBE_GATEWAY zeebe-zeebe-gateway:26500
COPY main.py .
ENTRYPOINT ["python3", "-u", "main.py"]
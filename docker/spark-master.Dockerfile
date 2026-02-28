FROM apache/spark:3.5.1
USER root
RUN mkdir -p /home/spark/.ivy2/cache \
    && chown -R spark:spark /home/spark/.ivy2 \
    && pip install --no-cache-dir requests

ENV PATH="${PATH}:/opt/spark/bin"
USER spark
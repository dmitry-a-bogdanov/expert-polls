FROM debian:buster-slim

RUN apt-get update && \
    apt-get --no-install-recommends install -y locales python3 python3-pip python3-setuptools python3-tzlocal python3-babel && \
    apt-get clean
RUN echo "388" | dpkg-reconfigure -plow locales
RUN pip3 install --no-cache-dir python-telegram-bot

COPY bot *.py /root/

RUN chmod 500 /root/bot

VOLUME /persistent/

ENTRYPOINT ["/root/bot"]

FROM debian:buster-slim

RUN apt-get update && \
    apt-get --no-install-recommends install -y locales python3 python3-pip python3-setuptools python3-socks && \
    apt-get clean && \
    (echo "388" | dpkg-reconfigure -plow locales) && \
    pip3 install --no-cache-dir python-telegram-bot && \
    apt-get purge -y python3-pip python3-setuptools && \
    apt-get autoremove -y --purge

COPY bot *.py /root/

RUN chmod 500 /root/bot

VOLUME /persistent/

ENTRYPOINT ["/root/bot"]

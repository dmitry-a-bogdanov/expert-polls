FROM ubuntu:noble-20241011

VOLUME /var/run/expert

RUN apt-get update && \
    apt-get --no-install-recommends install -y locales python3 python3-venv python3-pip && \
    apt-get clean && \
    localedef -i ru_RU -c -f UTF-8 -A /usr/share/locale/locale.alias ru_RU.UTF-8

WORKDIR /root/
RUN python3 -m venv ./venv
COPY requirements.txt /root/
RUN . /root/venv/bin/activate && \
    pip3 install -r requirements.txt

COPY bot run *.py /root/
RUN chmod 500 /root/bot /root/run

ENTRYPOINT ["/root/run"]

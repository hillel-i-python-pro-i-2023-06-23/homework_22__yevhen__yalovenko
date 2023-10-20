FROM python:3.11

ENV PYTHONUNBUFFERED=1

ARG WORKDIR=/wd
ARG USER=user

WORKDIR ${WORKDIR}

RUN useradd --system ${USER} && \
    chown --recursive ${USER} ${WORKDIR}

RUN apt update && apt upgrade -y

COPY --chown=${USER} requirements.txt requirements.txt
COPY --chown=${USER} requirements requirements

RUN pip install --upgrade pip && \
    pip install --requirement requirements/production.txt

COPY --chown=${USER} ./crawler crawler
COPY --chown=${USER} ./run.py run.py

USER ${USER}

ENTRYPOINT ["python", "run.py"]

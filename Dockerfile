FROM ubuntu:latest
LABEL authors="Komnenos"

ENTRYPOINT ["top", "-b"]
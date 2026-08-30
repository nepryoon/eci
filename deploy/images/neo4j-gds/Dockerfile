# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM neo4j@sha256:037cf5756f0135cbfd66b739b6df7c7c4bb100f9ce11602f6f9538e17e02c74d

ADD --checksum=sha256:246e3fbbbf733b4def1e7b0a9740a2309f6605ee8a7b46b29fe1de56d0a4b47c \
    https://graphdatascience.ninja/neo4j-graph-data-science-2.13.12.jar \
    /opt/eci/neo4j-graph-data-science-2.13.12.jar

RUN chmod 0644 /opt/eci/neo4j-graph-data-science-2.13.12.jar && \
    sed -i 's#/var/lib/neo4j/products/neo4j-graph-data-science-\*.jar#/opt/eci/neo4j-graph-data-science-2.13.12.jar#' \
      /startup/neo4j-plugins.json

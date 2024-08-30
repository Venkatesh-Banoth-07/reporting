FROM alpine

# NIRA Added missing ARG instructions
ARG SOURCE
ARG COMMIT_HASH
ARG COMMIT_ID
ARG BUILD_TIME

# NIRA Added the required LABEL instructions
LABEL source=${SOURCE}
LABEL commit_hash=${COMMIT_HASH}
LABEL commit_id=${COMMIT_ID}
LABEL build_time=${BUILD_TIME}

RUN apk add curl bash vim

ADD docker-entrypoint.sh /

CMD ["/docker-entrypoint.sh"]

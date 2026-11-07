FROM apache/kafka:3.9.2@sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02
ARG TARGETARCH
ARG VERSION
ARG REVISION
COPY linux/${TARGETARCH}/kaferry /usr/local/bin/kaferry
COPY linux/${TARGETARCH}/LICENSE linux/${TARGETARCH}/THIRD_PARTY_NOTICES.txt /usr/share/doc/kaferry/
COPY --chmod=755 demo.sh /usr/local/bin/kaferry-demo
LABEL org.opencontainers.image.source="https://github.com/pagrus7/kaferry" \
      org.opencontainers.image.licenses="MIT AND Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
ENV KAFKA_HEAP_OPTS="-Xms128m -Xmx512m"
USER appuser
ENTRYPOINT ["timeout", "--signal=TERM", "--kill-after=10s", "180s", "/usr/local/bin/kaferry-demo"]
CMD []

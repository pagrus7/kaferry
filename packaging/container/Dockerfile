FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
ARG TARGETARCH
ARG VERSION
ARG REVISION
COPY linux/${TARGETARCH}/kaferry /usr/local/bin/kaferry
COPY linux/${TARGETARCH}/LICENSE linux/${TARGETARCH}/THIRD_PARTY_NOTICES.txt /usr/share/doc/kaferry/
LABEL org.opencontainers.image.source="https://github.com/pagrus7/kaferry" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
ENV HOME=/tmp
WORKDIR /work
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kaferry"]
CMD ["--version"]

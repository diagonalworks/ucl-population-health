# Create a docker image for the synthetic population generator, by first
# building it from source in a separate container.
FROM ubuntu:jammy AS population-build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -yq install make curl libgdal-dev
RUN curl -L -O https://go.dev/dl/go1.20.3.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.20.3.linux-amd64.tar.gz && rm go1.20.3.linux-amd64.tar.gz
COPY Makefile /build/Makefile
COPY src /build/src
COPY deps /build/deps
COPY world /build/world
COPY data /build/data
ENV PATH=/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN make -C /build
FROM ubuntu:jammy
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -yq install libgdal-dev
COPY --from=population-build /build/bin/population /diagonal/bin/
COPY --from=population-build /build/world /diagonal/world
COPY --from=population-build /build/data /diagonal/data
COPY --from=population-build /build/cached /diagonal/cached
WORKDIR /diagonal
CMD ["/diagonal/bin/population"]
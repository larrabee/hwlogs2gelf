FROM alpine:edge AS build
WORKDIR /go/src/github.com/larrabee/hwlogs2gelf
RUN apk add -U --no-cache go git fftw-dev musl-dev dep
ENV GOPATH /go
COPY ./ ./
RUN dep ensure
RUN go build .


FROM alpine:edge AS release
WORKDIR /usr/local/bin/
COPY --from=build /go/src/github.com/larrabee/hwlogs2gelf/hwlogs2gelf ./
RUN apk add -U --no-cache ca-certificates
COPY ./config.yml ./
ENTRYPOINT ["/usr/local/bin/hwlogs2gelf"]
CMD ["config.yml"]


FROM golang:latest

RUN go get -u cloud.google.com/go/bigquery && \
	go get -u github.com/gdamore/tcell

ADD . /src
RUN mkdir -p /go/src/github.com/jonmorehouse && \
	ln -s /src /go/src/github.com/jonmorehouse/github-hilbert && \
	mkdir /output && \
	cd /src && \
	CGO_ENABLED=0 GOOS=linux go build -o /output/github-hilbert .

FROM alpine:latest
COPY --from=0 /output/github-hilbert /bin
ENTRYPOINT ["/bin/github-hilbert"]

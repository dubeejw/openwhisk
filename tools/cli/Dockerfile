FROM golang

ARG GOOS=darwin
ARG GOARCH=amd64

ADD . /src/github.ibm.com/

RUN cd /src/github.ibm.com/go-whisk-cli && go get -d -v
RUN cd /src/github.ibm.com/go-whisk-cli && go build -o wsk main.go

RUN ls /src/github.ibm.com/go-whisk-cli

#RUN cd /src/github.ibm.com/go-whisk-cli/ && ./wsk -h